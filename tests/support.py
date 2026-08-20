from __future__ import annotations

import contextlib
import json
import importlib
import sys
import types
from typing import Any


class FakeDeepDiff(dict):
    def __init__(self, existing: dict, incoming: dict, **_: Any) -> None:
        super().__init__()
        if existing != incoming:
            self["old"] = existing
            self["new"] = incoming

    def to_json(self) -> str:
        return json.dumps(self, default=str, sort_keys=True)


def install_dependency_stubs() -> None:
    if "deepdiff" not in sys.modules:
        with contextlib.suppress(ImportError):
            importlib.import_module("deepdiff")
        if "deepdiff" not in sys.modules:
            deepdiff = types.ModuleType("deepdiff")
            deepdiff.DeepDiff = FakeDeepDiff
            sys.modules["deepdiff"] = deepdiff

    if "neo4j" not in sys.modules:
        with contextlib.suppress(ImportError):
            importlib.import_module("neo4j")
        if "neo4j" not in sys.modules:
            neo4j = types.ModuleType("neo4j")
            neo4j.AsyncDriver = object
            neo4j.AsyncManagedTransaction = object

            class AsyncGraphDatabase:
                @staticmethod
                def driver(*args: Any, **kwargs: Any) -> object:
                    return object()

            neo4j.AsyncGraphDatabase = AsyncGraphDatabase
            sys.modules["neo4j"] = neo4j

    if "neomodel" not in sys.modules:
        with contextlib.suppress(ImportError):
            importlib.import_module("neomodel")
        if "neomodel" not in sys.modules:
            neomodel = types.ModuleType("neomodel")

            class Config:
                DATABASE_URL = None

            class Adb:
                async def cypher_query(self, *args: Any, **kwargs: Any) -> tuple[list, None]:
                    return [], None

                async def install_all_labels(self) -> None:
                    return None

            neomodel.config = Config()
            neomodel.adb = Adb()
            sys.modules["neomodel"] = neomodel


class FakeAsyncResult:
    def __init__(self, rows: list[Any] | None = None) -> None:
        self._rows = rows or []
        self._index = 0
        self.consumed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self._rows):
            raise StopAsyncIteration
        row = self._rows[self._index]
        self._index += 1
        return row

    async def consume(self) -> None:
        self.consumed = True


class FakeTx:
    def __init__(self, results: list[FakeAsyncResult]) -> None:
        self.results = list(results)
        self.calls: list[dict[str, Any]] = []

    async def run(self, cypher: str, **params: Any) -> FakeAsyncResult:
        self.calls.append({"cypher": cypher, "params": params})
        if self.results:
            return self.results.pop(0)
        return FakeAsyncResult()


class NullLog:
    def info(self, *args: Any, **kwargs: Any) -> None:
        return None

    def warning(self, *args: Any, **kwargs: Any) -> None:
        return None

    def error(self, *args: Any, **kwargs: Any) -> None:
        return None
