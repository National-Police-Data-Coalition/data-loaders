from __future__ import annotations

import logging
import unittest

from tests.support import install_dependency_stubs

install_dependency_stubs()

from loader.db import runner


async def async_lines(values):
    for value in values:
        yield value


class FakeSession:
    def __init__(self, calls):
        self.calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute_write(self, handler, batch, log):
        self.calls.append(batch)
        return await handler(self, batch, log=log)


class FakeDriver:
    def __init__(self):
        self.calls = []

    def session(self):
        return FakeSession(self.calls)


class LoadModelGroupTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_on_error_raises_handler_exception(self):
        old_handler = runner.REGISTRY.get("test_model")

        async def broken_handler(tx, batch, log):
            raise RuntimeError("boom")

        runner.REGISTRY["test_model"] = broken_handler
        logging.disable(logging.CRITICAL)
        try:
            with self.assertRaisesRegex(RuntimeError, "boom"):
                await runner.load_model_group(
                    FakeDriver(),
                    "test_model",
                    async_lines(['{"id": 1}', '{"id": 2}']),
                    batch_size=1,
                    concurrency=1,
                    stop_on_error=True,
                )
        finally:
            logging.disable(logging.NOTSET)
            if old_handler is None:
                runner.REGISTRY.pop("test_model", None)
            else:
                runner.REGISTRY["test_model"] = old_handler

    async def test_invalid_json_lines_are_skipped(self):
        old_handler = runner.REGISTRY.get("test_model")
        seen = []

        async def collecting_handler(tx, batch, log):
            seen.extend(batch)

        runner.REGISTRY["test_model"] = collecting_handler
        logging.disable(logging.CRITICAL)
        try:
            await runner.load_model_group(
                FakeDriver(),
                "test_model",
                async_lines(['{"id": 1}', "not json", '{"id": 2}']),
                batch_size=3,
                concurrency=1,
                stop_on_error=True,
            )
        finally:
            logging.disable(logging.NOTSET)
            if old_handler is None:
                runner.REGISTRY.pop("test_model", None)
            else:
                runner.REGISTRY["test_model"] = old_handler

        self.assertEqual(seen, [{"id": 1}, {"id": 2}])


if __name__ == "__main__":
    unittest.main()
