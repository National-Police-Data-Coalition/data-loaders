from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Sequence, Tuple

from dotenv import load_dotenv
from neomodel import config, adb

import loader.ingest # noqa: F401
from loader.ingest.base import REGISTRY
from loader.schema import install_schema

# ------------------------------------------------------------
# Config and logging
# ------------------------------------------------------------

load_dotenv()

MODEL_ORDER: tuple[str, ...] = (
    "agency",
    "unit",
    "officer",
    "complaint",
    "litigation",
)


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

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(formatter)
    root.addHandler(sh)

    if log_to_file:
        os.makedirs(log_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        log_path = os.path.join(log_dir, f"{ts}_load.log")
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        root.addHandler(fh)
        logging.info("File logging enabled: %s", log_path)


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


# ------------------------------------------------------------
# IO: group-by-model spooling
# ------------------------------------------------------------

@contextmanager
def iter_jsonl_grouped_lines(
    input_path: str | os.PathLike,
    model_order: Sequence[str] = MODEL_ORDER,
    unknown_bucket: str = "_unknown",
) -> Iterator[Iterator[Tuple[str, Iterable[str]]]]:
    """Yield (model, iterable_of_lines) grouped by obj['model'].

    - Single pass over file
    - Uses temp spool files per model
    - Cleans up temp files on exit

    Output order:
      model_order first, then any extra models encountered (sorted).
    """

    in_path = Path(input_path)
    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    spools: Dict[str, str] = {}

    def spool_for(model: str) -> str:
        if model not in spools:
            tf = tempfile.NamedTemporaryFile(
                mode="w", delete=False, encoding="utf-8", newline="\n"
            )
            tf.close()
            spools[model] = tf.name
        return spools[model]

    total = 0
    bad_json = 0
    missing_model = 0

    try:
        logging.info("Spooling JSONL lines by model: %s", in_path)

        with in_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line.strip():
                    continue
                total += 1

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    bad_json += 1
                    continue

                model = obj.get("model") or unknown_bucket
                if model == unknown_bucket:
                    missing_model += 1

                spool_path = spool_for(str(model))
                with open(spool_path, "a", encoding="utf-8", newline="\n") as out:
                    out.write(line)
                    out.write("\n")

        ordered = list(model_order)
        extras = sorted(m for m in spools.keys() if m not in set(ordered))
        concat_order = ordered + extras

        logging.info(
            "Spool complete. total=%d bad_json=%d missing_model=%d models=%s",
            total,
            bad_json,
            missing_model,
            concat_order,
        )

        def group_iterator() -> Iterator[Tuple[str, Iterable[str]]]:
            for model in concat_order:
                if model not in spools:
                    continue

                def lines_iter(spool_path: str) -> Iterator[str]:
                    with open(spool_path, "r", encoding="utf-8") as part:
                        for l in part:
                            yield l.rstrip("\n")

                yield model, lines_iter(spools[model])

        yield group_iterator()
    finally:
        for _, path in spools.items():
            try:
                os.unlink(path)
            except OSError:
                logging.warning("Failed to remove spool file: %s", path)


def yield_batches(lines: Iterable[str], batch_size: int) -> Iterator[List[str]]:
    """Yield lists of up to batch_size lines."""

    batch: List[str] = []
    for line in lines:
        if not line:
            continue
        batch.append(line)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


# ------------------------------------------------------------
# Ingest
# ------------------------------------------------------------

Handler = Callable[[List[dict[str, Any]]], Any]


def _write_tx_cm():
    """Return the write transaction context manager for adb.

    Sync docs show db.write_transaction / db.transaction context managers.
    Async neomodel mirrors this API on `adb`, but used with `async with`.
    """

    return getattr(adb, "write_transaction", None) or getattr(adb, "transaction")


async def ingest_batch(model: str, objs: List[dict[str, Any]]) -> None:
    handler: Handler | None = REGISTRY.get(model)
    if handler is None:
        logging.warning("No handler registered for model: %s", model)
        return

    tx = _write_tx_cm()
    if tx is None:
        raise RuntimeError(
            "adb has no transaction context manager (expected .write_transaction or .transaction)"
        )

    # One transaction per batch.
    async with tx:
        # Convention: handlers accept a *batch* (list of dicts)
        # so they can use UNWIND / neomodel batch helpers, etc.
        await handler(objs)


async def load_jsonl_to_neo4j(
    jsonl_filename: str | os.PathLike,
    batch_size: int = 500,
    model_order: Sequence[str] = MODEL_ORDER,
    stop_on_error: bool = False,
) -> None:
    """Main ingest loop.

    Group by model (spooled), then process each group in batches.
    """

    with iter_jsonl_grouped_lines(jsonl_filename, model_order=model_order) as groups:
        for model, lines in groups:
            logging.info("Loading model group: %s", model)

            for raw_batch in yield_batches(lines, batch_size=batch_size):
                objs: List[dict[str, Any]] = []
                for line in raw_batch:
                    try:
                        objs.append(json.loads(line))
                    except json.JSONDecodeError:
                        logging.warning("Skipping invalid JSON line in model %s", model)

                if not objs:
                    continue

                try:
                    await ingest_batch(model, objs)
                except Exception:
                    logging.exception(
                        "Batch ingest failed: model=%s size=%d", model, len(objs)
                    )
                    if stop_on_error:
                        raise


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

@dataclass
class Args:
    cmd: str
    input_file: str
    logging: str | None
    batch_size: int
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

        return Args(
            cmd=ns.cmd,
            input_file=ns.input_file,
            logging=ns.logging,
            batch_size=ns.batch_size,
            stop_on_error=bool(ns.stop_on_error),
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
    setup_logging(args.logging)
    configure_neomodel()


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
        await load_jsonl_to_neo4j(
            args.input_file,
            batch_size=args.batch_size,
            stop_on_error=args.stop_on_error,
        )
    except Exception as e:
        logging.error("Load failed: %s", e)
        return 2

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    start_time = time.time()
    code = main()
    end_time = time.time()
    print(f"Completed in {end_time - start_time} seconds")
    raise SystemExit(code)
