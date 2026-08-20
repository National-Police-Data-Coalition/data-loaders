import importlib.util
from pathlib import Path
from datetime import datetime, timezone, timedelta
import unittest

from loader.utils.deterministic_uid import (
    canonical_change_timestamp,
    canonical_change_url,
    det_change_uid,
    deterministic_node_uid,
)


def load_change_module():
    module_path = Path(__file__).resolve().parents[1] / "loader" / "ingest" / "change.py"
    spec = importlib.util.spec_from_file_location("ingest_change", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


change = load_change_module()


class LatestChangeTimestampCypherTests(unittest.TestCase):
    def test_checks_change_shape_only(self):
        cypher = change.latest_change_timestamp_cypher(
            "o",
            "s",
            result_alias="last_officer_change",
            change_alias="officer_change",
        )

        self.assertIn(
            "OPTIONAL MATCH (o)<-[:CHANGE_TO]-(officer_change:Change)-[:ATTRIBUTED_TO]->(s)",
            cypher,
        )
        self.assertIn("WHERE o IS NOT NULL", cypher)
        self.assertIn("WITH row, o, s, max(officer_change.timestamp) AS last_officer_change", cypher)
        self.assertNotIn("UPDATED_BY", cypher)
        self.assertNotIn("RETURN CASE", cypher)

    def test_uses_supplied_aliases_in_subquery_scope(self):
        cypher = change.latest_change_timestamp_cypher("e", "source")

        self.assertIn("WITH row, e, source, max(change.timestamp) AS last_ts", cypher)

    def test_supports_custom_carry_aliases_for_nested_scopes(self):
        cypher = change.latest_change_timestamp_cypher(
            "e",
            "s",
            result_alias="emp_last_ts",
            carry_aliases=("i", "emp", "a", "u", "e", "s"),
        )

        self.assertIn("WITH i, emp, a, u, e, s, max(change.timestamp) AS emp_last_ts", cypher)


class MergeChangeCypherTests(unittest.TestCase):
    def test_merges_change_node_relationships(self):
        cypher = change.merge_change_cypher("o", "s", "officer_change")

        self.assertIn("MERGE (officer_change:Change {uid: row.change_uid})", cypher)
        self.assertIn("MERGE (o)<-[:CHANGE_TO]-(officer_change)", cypher)
        self.assertIn("MERGE (officer_change)-[:ATTRIBUTED_TO]->(s)", cypher)
        self.assertNotIn("UPDATED_BY", cypher)

    def test_uses_precomputed_hashed_uid(self):
        cypher = change.merge_change_cypher("target", "source", "target_change")

        self.assertIn("MERGE (target_change:Change {uid: row.change_uid})", cypher)
        self.assertNotIn("elementId(target)", cypher)
        self.assertNotIn("AS target_change_uid", cypher)

    def test_supports_custom_row_alias_and_diff_expression(self):
        cypher = change.merge_change_cypher(
            "e",
            "s",
            "employment_change",
            row_alias="emp",
            diff_expr="emp.diff",
        )

        self.assertIn("datetime(emp.scraped_dt)", cypher)
        self.assertIn("employment_change.diff = emp.diff", cypher)
        self.assertIn("MERGE (employment_change:Change {uid: emp.change_uid})", cypher)
        self.assertNotIn("row.diff", cypher)

    def test_det_change_uid_hashes_canonical_identity(self):
        uid = det_change_uid(
            "target-uid",
            "source-uid",
            datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
            "https://example.test",
        )

        self.assertEqual(len(uid), 64)
        self.assertRegex(uid, r"^[0-9a-f]{64}$")
        self.assertEqual(
            uid,
            det_change_uid(
                "target-uid",
                "source-uid",
                datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
                "https://example.test",
            ),
        )

    def test_det_change_uid_normalizes_equivalent_timestamps(self):
        self.assertEqual(
            det_change_uid(
                "target-uid",
                "source-uid",
                "2024-01-02T03:04:05.000000Z",
                None,
            ),
            det_change_uid(
                "target-uid",
                "source-uid",
                datetime(
                    2024,
                    1,
                    1,
                    21,
                    4,
                    5,
                    tzinfo=timezone(timedelta(hours=-6)),
                ),
                None,
            ),
        )

    def test_det_change_uid_changes_when_identity_inputs_change(self):
        base = det_change_uid(
            "target-uid",
            "source-uid",
            "2024-01-02T03:04:05Z",
            "https://example.test",
        )

        self.assertNotEqual(
            base,
            det_change_uid(
                "other-target",
                "source-uid",
                "2024-01-02T03:04:05Z",
                "https://example.test",
            ),
        )
        self.assertNotEqual(
            base,
            det_change_uid(
                "target-uid",
                "other-source",
                "2024-01-02T03:04:05Z",
                "https://example.test",
            ),
        )
        self.assertNotEqual(
            base,
            det_change_uid(
                "target-uid",
                "source-uid",
                "2024-01-02T03:04:06Z",
                "https://example.test",
            ),
        )
        self.assertNotEqual(
            base,
            det_change_uid(
                "target-uid",
                "source-uid",
                "2024-01-02T03:04:05Z",
                "https://other.example.test",
            ),
        )

    def test_canonical_change_timestamp_normalizes_inputs_to_utc(self):
        expected = "2024-01-02T03:04:05.123456Z"

        self.assertEqual(
            canonical_change_timestamp("2024-01-02T03:04:05.123456Z"),
            expected,
        )
        self.assertEqual(
            canonical_change_timestamp("2024-01-01T21:04:05.123456-06:00"),
            expected,
        )
        self.assertEqual(
            canonical_change_timestamp(datetime(2024, 1, 2, 3, 4, 5, 123456)),
            expected,
        )

    def test_canonical_change_url_defaults_none_to_empty_string(self):
        self.assertEqual(canonical_change_url(None), "")
        self.assertEqual(canonical_change_url(" https://example.test "), " https://example.test ")

    def test_deterministic_node_uid_is_stable_for_same_parts(self):
        uid = deterministic_node_uid("agency", "Albany PD", "NY")

        self.assertEqual(len(uid), 64)
        self.assertRegex(uid, r"^[0-9a-f]{64}$")
        self.assertEqual(uid, deterministic_node_uid("agency", "Albany PD", "NY"))


if __name__ == "__main__":
    unittest.main()
