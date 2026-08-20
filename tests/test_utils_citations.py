from __future__ import annotations

from datetime import datetime, timezone
import unittest

from tests.support import install_dependency_stubs

install_dependency_stubs()

from loader.utils.citations import detect_diff_dict, parse_scraped_at


class CitationUtilsTests(unittest.TestCase):
    def test_parse_scraped_at_returns_utc_datetime(self):
        self.assertEqual(
            parse_scraped_at("2024-01-02 03:04:05"),
            datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        )

    def test_detect_diff_ignores_uids_and_null_values(self):
        diff = detect_diff_dict(
            {
                "uid": "abc",
                "element_id_property": "internal",
                "name": "Central",
                "email": None,
            },
            {
                "name": "Central",
                "email": None,
            },
        )

        self.assertFalse(diff)

    def test_detect_diff_reports_meaningful_changes(self):
        diff = detect_diff_dict({"name": "Old"}, {"name": "New"})

        self.assertTrue(diff)
        self.assertIn("New", diff.to_json())


if __name__ == "__main__":
    unittest.main()
