from __future__ import annotations

import os
import json
import asyncio
import logging
import tempfile
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import (
    AsyncIterable, AsyncIterator,
    Dict, Iterable, Iterator, List, Tuple, Sequence
)


# ------------------------------------------------------------
# Sync spooling of JSONL lines by model
# ------------------------------------------------------------

@contextmanager
def iter_jsonl_grouped_lines(
    input_path: str | os.PathLike,
    model_order: Sequence[str],
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
# Async spooling of JSONL lines by model
# ------------------------------------------------------------

def _spool_sync(
    input_path: str | os.PathLike,
    model_order: Sequence[str],
    unknown_bucket: str,
) -> tuple[dict[str, str], list[str], int, int, int]:
    """
    Spool the input file into per-model temp files.

    Returns:
      (spools, concat_order, total, bad_json, missing_model)
    """
    in_path = Path(input_path)
    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    spools: dict[str, str] = {}
    handles: dict[str, tempfile._TemporaryFileWrapper] = {}

    def get_handle(model: str):
        if model not in handles:
            tf = tempfile.NamedTemporaryFile(
                mode="w", delete=False, encoding="utf-8", newline="\n"
            )
            handles[model] = tf
            spools[model] = tf.name
        return handles[model]

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

                h = get_handle(str(model))
                h.write(line)
                h.write("\n")

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

        return spools, concat_order, total, bad_json, missing_model
    finally:
        for h in handles.values():
            try:
                h.close()
            except OSError:
                pass


def _read_chunk(f, max_lines: int) -> list[str]:
    out: list[str] = []
    for _ in range(max_lines):
        line = f.readline()
        if not line:
            break
        out.append(line.rstrip("\n"))
    return out


async def aiter_file_lines(
    path: str | os.PathLike,
    *,
    chunk_lines: int = 4096,
) -> AsyncIterator[str]:
    """
    Async-iterate a text file by reading chunks in a thread, so the event loop
    isn't blocked by file IO.
    """
    f = open(path, "r", encoding="utf-8")
    try:
        while True:
            chunk = await asyncio.to_thread(_read_chunk, f, chunk_lines)
            if not chunk:
                break
            for line in chunk:
                yield line
    finally:
        try:
            f.close()
        except OSError:
            pass


@asynccontextmanager
async def aiter_jsonl_grouped_lines(
    input_path: str | os.PathLike,
    model_order: Sequence[str],
    unknown_bucket: str = "_unknown",
) -> AsyncIterator[AsyncIterator[Tuple[str, AsyncIterable[str]]]]:
    """
    Async context manager version of iter_jsonl_grouped_lines.

    - Spooling happens in a background thread (asyncio.to_thread)
    - Each spool file is read via aiter_file_lines (chunked thread reads)
    """
    spools: dict[str, str] = {}
    concat_order: list[str] = []
    try:
        spools, concat_order, _, _, _ = await asyncio.to_thread(
            _spool_sync, input_path, model_order, unknown_bucket
        )

        async def group_iterator() -> AsyncIterator[Tuple[str, AsyncIterable[str]]]:
            for model in concat_order:
                path = spools.get(model)
                if not path:
                    continue
                yield model, aiter_file_lines(path)

        yield group_iterator()
    finally:
        # Cleanup temp files (also via thread to avoid any IO hiccup)
        for path in spools.values():
            try:
                await asyncio.to_thread(os.unlink, path)
            except OSError:
                logging.warning("Failed to remove spool file: %s", path)


async def ayield_batches(
    lines: AsyncIterable[str],
    batch_size: int,
) -> AsyncIterator[List[str]]:
    batch: List[str] = []
    async for line in lines:
        if not line:
            continue
        batch.append(line)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
