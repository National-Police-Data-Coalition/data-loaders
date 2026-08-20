from __future__ import annotations

import unittest

from tests.support import install_dependency_stubs

install_dependency_stubs()

from loader.cli import Args, parse_args, parse_bool


class CliTests(unittest.TestCase):
    def test_parse_bool_accepts_common_truthy_values(self):
        for value in ("1", "true", "yes", "y", "on"):
            with self.subTest(value=value):
                self.assertTrue(parse_bool(value))

        self.assertFalse(parse_bool(None))
        self.assertTrue(parse_bool(None, default=True))
        self.assertFalse(parse_bool("false"))

    def test_parse_load_args(self):
        args = parse_args(["load", "input.jsonl", "--batch-size", "10", "--concurrency", "2", "--stop-on-error"])

        self.assertEqual(
            args,
            Args(
                cmd="load",
                input_file="input.jsonl",
                logging=None,
                batch_size=10,
                concurrency=2,
                stop_on_error=True,
            ),
        )

    def test_rejects_invalid_logging_level(self):
        with self.assertRaises(SystemExit):
            parse_args(["load", "input.jsonl", "--logging", "VERBOSE"])

    def test_rejects_invalid_batch_size_and_concurrency(self):
        with self.assertRaises(SystemExit):
            parse_args(["load", "input.jsonl", "--batch-size", "0"])
        with self.assertRaises(SystemExit):
            parse_args(["load", "input.jsonl", "--concurrency", "0"])


if __name__ == "__main__":
    unittest.main()
