from neo4j import AsyncDriver
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, Callable, Iterable, List, Optional, Awaitable, AsyncIterable
from loader.ingest.base import REGISTRY
from loader.io.spool import ayield_batches


Handler = Callable[[List[dict[str, Any]]], Awaitable[Any]]


class CtxAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        ctx = " ".join(f"{k}={v}" for k, v in self.extra.items())
        return f"{ctx} {msg}", kwargs


async def load_model_group(
    driver: AsyncDriver,
    model: str,
    lines: AsyncIterable[str],
    *,
    batch_size: int,
    concurrency: int,
    stop_on_error: bool,
) -> None:
    if concurrency < 1:
        concurrency = 1
    
    q: asyncio.Queue[Optional[List[dict[str, Any]]]] = asyncio.Queue(
        maxsize=concurrency * 2
    )
    err: Exception | None = None
    stop_event = asyncio.Event()

    async def producer() -> None:
        batch_seq = 0
        nonlocal err
        try:
            async for raw_batch in ayield_batches(lines, batch_size=batch_size):
                batch_seq += 1
                if stop_event.is_set():
                    break

                objs: List[dict[str, Any]] = []
                for line in raw_batch:
                    if not line:
                        continue
                    try:
                        objs.append(json.loads(line))
                    except json.JSONDecodeError:
                        logging.warning(
                            "Skipping invalid JSON line in model %s", model
                        )
                if objs:
                    logging.debug(
                        "[%s prod] queued batch=%d size=%d",
                        model, batch_seq, len(objs)
                    )
                    await q.put(objs)
        except Exception as e:
            err = e
            stop_event.set()
        finally:
            # tell workers to exit
            for _ in range(concurrency):
                await q.put(None)

    async def worker(worker_id: int) -> None:
        log = CtxAdapter(
            logging.getLogger(__name__),
            {"model": model, "worker": worker_id}
        )
        batch_seq = 0
        async with driver.session() as session:
            while True:
                batch = await q.get()
                try:
                    if batch is None:
                        return
                    
                    batch_seq += 1
                    log.info(
                        "start batch=%d size=%d", batch_seq, len(batch)
                    )
                    await session.execute_write(REGISTRY[model], batch, log=log)

                    log.info(
                        "done batch=%d size=%d", batch_seq, len(batch)
                    )
                except Exception:
                    logging.exception(
                        "[%s w=%d b=%d] error processing batch",
                        model, worker_id, batch_seq
                    )
                finally:
                    q.task_done()
    
    def _worker_done(t: asyncio.Task) -> None:
        nonlocal err
        exc = t.exception()
        if exc is not None and err is None:
            err = exc
            stop_event.set()
            for _ in range(concurrency):
                try:
                    q.put_nowait(None)
                except asyncio.QueueFull:
                    pass

    prod_task = asyncio.create_task(producer())
    worker_tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]

    for t in worker_tasks:
        t.add_done_callback(_worker_done)

    # wait for producer to finish
    await prod_task
    await q.join()

    await asyncio.gather(*worker_tasks)

    if err is not None:
        if stop_on_error:
            raise err
        else:
            logging.error("Error during loading model %s: %s", model, err)