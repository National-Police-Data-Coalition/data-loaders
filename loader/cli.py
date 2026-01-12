from __future__ import annotations

import os
import sys
import time
import queue
import logging
import asyncio
import argparse
from logging.handlers import QueueHandler, QueueListener
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase
from neomodel import config, adb

import loader.ingest # noqa: F401
from loader.ingest.base import REGISTRY
from loader.schema import install_schema
from loader.io.spool import aiter_jsonl_grouped_lines
from loader.db.runner import load_model_group

# ------------------------------------------------------------
# Config and logging
# ------------------------------------------------------------

load_dotenv()

def env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def parse_bool(s: str | None, default: bool = False) -> bool:
    if s is None:
        return default
    return s.lower() in ("1", "true", "yes", "y", "on")


def setup_logging(level_name: str | None = None) -> None:
    """Configure logging.

    Env:
      LOG_LEVEL=INFO|DEBUG|...  (default ERROR unless overridden by CLI)
      LOG_TO_FILE=true|false     (default false)
      LOG_DIR=logs               (default logs)
    """

    env_level = (env("LOG_LEVEL", "ERROR") or "ERROR").upper()
    level_name = (level_name or env_level).upper()
    level = getattr(logging, level_name, logging.ERROR)

    log_to_file = parse_bool(env("LOG_TO_FILE"), default=False)
    log_dir = env("LOG_DIR", "logs") or "logs"

    fmt = "%(asctime)s %(levelname)s: %(message)s"
    formatter = logging.Formatter(fmt)

    handlers: list[logging.Handler] = []

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(formatter)
    handlers.append(sh)

    if log_to_file:
        os.makedirs(log_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        log_path = os.path.join(log_dir, f"{ts}_load.log")

        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        handlers.append(fh)
    
    log_q: queue.Queue[logging.LogRecord] = queue.Queue()
    listener = QueueListener(log_q, *handlers, respect_handler_level=True)
    listener.start()

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.propagate = False
    root.addHandler(QueueHandler(log_q))

    if log_to_file:
        logging.info("File logging enabled: %s", log_path)
    
    return listener


def configure_neomodel() -> None:
    """Configure neomodel connection.
    """

    neo_url = "bolt://{user}:{pw}@{uri}".format(
        user=env("GRAPH_USER", "neo4j"),
        pw=env("GRAPH_PASSWORD"),
        uri=env("GRAPH_NM_URI")
    )
    config.DATABASE_URL = neo_url

    user = env("GRAPH_USER", "neo4j")
    password = env("GRAPH_PASSWORD")
    uri = env("GRAPH_NM_URI")
    scheme = env("GRAPH_SCHEME", "bolt")

    if not all([user, password, uri]):
        raise RuntimeError(
            "Missing Graph connection info in environment variables."
        )
    database_url = f"{scheme}://{user}:{password}@{uri}"

    config.DATABASE_URL = database_url


def make_driver(concurrency: int = 4) -> AsyncGraphDatabase.driver:
    user = env("GRAPH_USER", "neo4j")
    password = env("GRAPH_PASSWORD")
    uri = env("GRAPH_NM_URI")
    scheme = env("GRAPH_SCHEME", "bolt")
    if not all([user, password, uri]):
        raise RuntimeError(
            "Missing Graph connection info in environment variables."
        )
    return AsyncGraphDatabase.driver(
        f"{scheme}://{uri}",
        auth=(
            user,
            password
        ),
        max_connection_pool_size=concurrency * 2,
    )


# ------------------------------------------------------------
# Main Ingest Loop
# ------------------------------------------------------------


MODEL_ORDER: tuple[str, ...] = (
    "agency",
    "unit",
    "officer",
    "complaint",
    "litigation",
)


async def load_jsonl_to_neo4j(
    jsonl_filename: str | os.PathLike,
    batch_size: int = 500,
    model_order: Sequence[str] = MODEL_ORDER,
    concurrency: int = 4,
    stop_on_error: bool = False,
    driver: AsyncGraphDatabase.driver | None = None,
) -> None:
    """Main ingest loop.

    Group by model (spooled), then process each group in batches.
    """
    async with aiter_jsonl_grouped_lines(
        jsonl_filename,
        model_order=model_order) as groups:
        async for model, lines in groups:
            logging.info("Loading model group: %s", model)

            await load_model_group(
                driver,
                model,
                lines,
                batch_size=batch_size,
                concurrency=concurrency,
                stop_on_error=stop_on_error,
            )



# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

@dataclass
class Args:
    cmd: str
    input_file: str
    logging: str | None
    batch_size: int
    concurrency: int
    stop_on_error: bool


def parse_args(argv: Sequence[str] | None = None) -> Args:
    parser = argparse.ArgumentParser(
        prog="loader",
        description="JSONL -> Neo4j loader")
    subparsers = parser.add_subparsers(
        dest="cmd", required=True)
    
    # ---- load ----
    load_parser = subparsers.add_parser(
        "load",
        help="Load data from JSONL file to Neo4j")

    load_parser.add_argument(
        "input_file",
        type=os.path.relpath,
        help="Input JSONL file to load data from",
    )
    load_parser.add_argument(
        "-l",
        "--logging",
        type=str,
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    load_parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of JSONL rows per write transaction (default: 500)",
    )
    load_parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Abort immediately on first ingest error (default: continue)",
    )
    load_parser.add_argument(
        "--concurrency",
        type=int,
        default= 4,
        help="Number of concurrent write transactions (default: 4)",
    )


    # ---- install-labels ----
    install_parser = subparsers.add_parser(
        "install-labels",
        help="Install Neo4j schema labels/indexes"
    )
    install_parser.add_argument(
        "-l",
        "--logging",
        type=str,
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )

    ns = parser.parse_args(argv)

    if ns.logging and ns.logging.upper() not in {
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    }:
        raise SystemExit(f"Invalid logging level: {ns.logging}")
    
    if ns.cmd == "load":
        if ns.batch_size < 1:
            raise SystemExit("--batch-size must be >= 1")
        if ns.concurrency < 1:
            raise SystemExit("--concurrency must be >= 1")

        return Args(
            cmd=ns.cmd,
            input_file=ns.input_file,
            logging=ns.logging,
            batch_size=ns.batch_size,
            stop_on_error=bool(ns.stop_on_error),
            concurrency=ns.concurrency,
        )
    elif ns.cmd == "install-labels":
        return Args(
            cmd=ns.cmd,
            input_file="",
            logging=ns.logging,
            batch_size=0,
            stop_on_error=False,
        )


async def async_main(args: Args) -> int:
    listener = setup_logging(args.logging)
    try:
        configure_neomodel()
        driver = make_driver(args.concurrency)


        if args.cmd == "install-labels":
            await install_schema()
            return 0

        # Verify Neo4j connection
        try:
            await adb.cypher_query("RETURN 1")
            logging.info("Successfully connected to Neo4j")
        except Exception as e:
            logging.error("Failed to connect to Neo4j: %s", e)
            print(f"Failed to connect to Neo4j: {e}", file=sys.stderr)
            return 1
        
        logging.info("Starting data load process")
        try:
            async with driver:
                await load_jsonl_to_neo4j(
                    args.input_file,
                    batch_size=args.batch_size,
                    concurrency=args.concurrency,
                    stop_on_error=args.stop_on_error,
                    driver=driver
                )
        except Exception as e:
            logging.error("Load failed: %s", e)
            return 2

        return 0
    finally:
        listener.stop()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    start_time = time.time()
    code = main()
    end_time = time.time()
    print(f"Completed in {end_time - start_time} seconds")
    raise SystemExit(code)
