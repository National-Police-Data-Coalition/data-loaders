from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from loader.io.spool import (
    aiter_jsonl_grouped_lines,
    ayield_batches,
    iter_jsonl_grouped_lines,
    yield_batches,
)


class SpoolTests(unittest.TestCase):
    def write_jsonl(self, rows: list[str]) -> Path:
        tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8")
        with tmp:
            tmp.write("\n".join(rows))
            tmp.write("\n")
        return Path(tmp.name)

    def test_groups_valid_lines_by_model_in_requested_order(self):
        path = self.write_jsonl(
            [
                json.dumps({"model": "officer", "id": 1}),
                "not json",
                "",
                json.dumps({"model": "agency", "id": 2}),
                json.dumps({"id": 3}),
                json.dumps({"model": "z_extra", "id": 4}),
            ]
        )
        try:
            with iter_jsonl_grouped_lines(path, model_order=("agency", "officer")) as groups:
                grouped = [(model, list(lines)) for model, lines in groups]

            self.assertEqual([model for model, _ in grouped], ["agency", "officer", "_unknown", "z_extra"])
            self.assertEqual(json.loads(grouped[0][1][0])["id"], 2)
            self.assertEqual(json.loads(grouped[2][1][0])["id"], 3)
        finally:
            path.unlink(missing_ok=True)

    def test_yield_batches_skips_blank_lines(self):
        self.assertEqual(
            list(yield_batches(["a", "", "b", "c"], batch_size=2)),
            [["a", "b"], ["c"]],
        )


class AsyncSpoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_grouped_lines_and_batches(self):
        tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8")
        with tmp:
            tmp.write(json.dumps({"model": "unit", "id": 1}) + "\n")
            tmp.write(json.dumps({"model": "agency", "id": 2}) + "\n")
        path = Path(tmp.name)
        try:
            async with aiter_jsonl_grouped_lines(path, model_order=("agency", "unit")) as groups:
                seen = []
                async for model, lines in groups:
                    batch_list = []
                    async for batch in ayield_batches(lines, batch_size=1):
                        batch_list.append(batch)
                    seen.append((model, batch_list))

            self.assertEqual([model for model, _ in seen], ["agency", "unit"])
            self.assertEqual(json.loads(seen[0][1][0][0])["id"], 2)
        finally:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
