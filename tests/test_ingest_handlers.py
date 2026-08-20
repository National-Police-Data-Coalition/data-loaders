from __future__ import annotations

import hashlib
from datetime import datetime, timezone
import unittest

from tests.support import FakeAsyncResult, FakeTx, NullLog, install_dependency_stubs

install_dependency_stubs()

from loader.ingest import agency, allegation, complaint, officer, unit
from loader.ingest.complaint_key import build_complaint_key


SCRAPED_AT = "2024-01-02 03:04:05"
SCRAPED_DT = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
COMPLAINT_KEY = hashlib.sha256("complaint\x1fsrc\x1fC1".encode("utf-8")).hexdigest()


class AgencyIngestTests(unittest.IsolatedAsyncioTestCase):
    async def test_agency_create_flattens_nested_address_and_writes_upsert(self):
        tx = FakeTx([FakeAsyncResult([(0, False, None, None)]), FakeAsyncResult()])

        await agency.upsert_agency_batch(
            tx,
            [
                {
                    "data": {
                        "name": "Albany PD",
                        "address": {
                            "street": "1 Main",
                            "city": "Albany",
                            "state": "NY",
                            "postal_code": "12207",
                        },
                        "phone": "555",
                    },
                    "source_uid": "src",
                    "url": "https://example.test",
                    "scraped_at": SCRAPED_AT,
                }
            ],
            NullLog(),
        )

        self.assertEqual(len(tx.calls), 2)
        rows = tx.calls[1]["params"]["rows"]
        self.assertEqual(rows[0]["name"], "Albany PD")
        self.assertEqual(rows[0]["props"]["hq_address"], "1 Main")
        self.assertEqual(rows[0]["hq_city"], "Albany")
        self.assertIsNone(rows[0]["diff"])

    async def test_agency_skips_stale_existing_row(self):
        tx = FakeTx([FakeAsyncResult([(0, True, {"phone": "555"}, SCRAPED_DT)]), FakeAsyncResult()])

        await agency.upsert_agency_batch(
            tx,
            [
                {
                    "data": {"name": "Albany PD", "hq_state": "NY", "phone": "999"},
                    "source_uid": "src",
                    "url": "https://example.test",
                    "scraped_at": SCRAPED_AT,
                }
            ],
            NullLog(),
        )

        self.assertEqual(len(tx.calls), 1)


class UnitIngestTests(unittest.IsolatedAsyncioTestCase):
    async def test_unit_requires_resolved_agency_before_upsert(self):
        tx = FakeTx([
            FakeAsyncResult([
                {
                    "record": {
                        "row_id": 0,
                        "agency_uid": None,
                        "exists": False,
                        "existing": None,
                        "last_ts": None,
                    }
                }
            ]),
            FakeAsyncResult(),
        ])

        await unit.upsert_unit_batch(
            tx,
            [
                {
                    "data": {"name": "Patrol", "hq_state": "NY"},
                    "agency": "Albany PD",
                    "source_uid": "src",
                    "url": "https://example.test",
                    "scraped_at": SCRAPED_AT,
                }
            ],
            NullLog(),
        )

        self.assertEqual(len(tx.calls), 1)


class ComplaintIngestTests(unittest.IsolatedAsyncioTestCase):
    async def test_complaint_create_includes_location_diff_key_used_by_cypher(self):
        tx = FakeTx([
            FakeAsyncResult([
                {
                    "record": {
                        "row_id": 0,
                        "complaint": None,
                        "exists": False,
                        "existing": None,
                        "existing_loc": None,
                        "last_ts": None,
                    }
                }
            ]),
            FakeAsyncResult(),
        ])

        await complaint.upsert_complaint_batch(
            tx,
            [
                {
                    "data": {
                        "record_id": "C1",
                        "category": "Use of force",
                        "location": {"city": "Albany", "state": "NY"},
                    },
                    "source_uid": "src",
                    "url": "https://example.test",
                    "scraped_at": SCRAPED_AT,
                }
            ],
            NullLog(),
        )

        rows = tx.calls[1]["params"]["rows"]
        self.assertIn("loc_diff", rows[0])
        self.assertIsNone(rows[0]["loc_diff"])
        self.assertEqual(rows[0]["props"]["complaint_key"], COMPLAINT_KEY)

    def test_complaint_key_uses_sha256_with_unambiguous_separator(self):
        self.assertEqual(build_complaint_key("src", "C1"), COMPLAINT_KEY)

    async def test_complaint_existing_compares_filtered_props_not_full_payload(self):
        tx = FakeTx([
            FakeAsyncResult([
                {
                    "record": {
                        "row_id": 0,
                        "complaint": "c-uid",
                        "exists": True,
                        "existing": {
                            "category": "Use of force",
                            "complaint_key": COMPLAINT_KEY,
                        },
                        "existing_loc": {"city": "Albany", "state": "NY"},
                        "last_ts": None,
                    }
                }
            ]),
            FakeAsyncResult(),
        ])

        await complaint.upsert_complaint_batch(
            tx,
            [
                {
                    "data": {
                        "record_id": "C1",
                        "category": "Use of force",
                        "location": {"city": "Albany", "state": "NY"},
                        "source_details": {"record_type": "government"},
                    },
                    "source_uid": "src",
                    "url": "https://example.test",
                    "scraped_at": SCRAPED_AT,
                }
            ],
            NullLog(),
        )

        self.assertEqual(len(tx.calls), 1)


class AllegationIngestTests(unittest.IsolatedAsyncioTestCase):
    async def test_allegation_missing_officer_state_id_is_allowed(self):
        tx = FakeTx([
            FakeAsyncResult([
                {
                    "record": {
                        "row_id": 0,
                        "complaint": "complaint-uid",
                        "officer": None,
                        "exists": False,
                        "existing": None,
                        "complainant": None,
                        "last_ts": None,
                    }
                }
            ]),
            FakeAsyncResult(),
        ])

        await allegation.upsert_allegation_batch(
            tx,
            [
                {
                    "data": {"record_id": "A1", "allegation": "Force"},
                    "complaint_id": "C1",
                    "complainant": {"age": 30},
                    "source_uid": "src",
                    "url": "https://example.test",
                    "scraped_at": SCRAPED_AT,
                }
            ],
            NullLog(),
        )

        prefetch_rows = tx.calls[0]["params"]["rows"]
        upsert_rows = tx.calls[1]["params"]["rows"]
        self.assertIsNone(prefetch_rows[0]["officer_id_name"])
        self.assertEqual(prefetch_rows[0]["complaint_key"], COMPLAINT_KEY)
        self.assertIsNone(upsert_rows[0]["officer_uid"])

    def test_civilian_change_uses_civilian_diff(self):
        self.assertIn("civilian_change.diff = row.civ_diff", allegation.UPSERT_CYPHER)


class OfficerIngestTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_employment_uses_fetched_match_state(self):
        tx = FakeTx([
            FakeAsyncResult([
                {
                    "record": {
                        "row_id": 0,
                        "sid": "sid-uid",
                        "exists": True,
                        "existing": {"first_name": "Ada", "last_name": "Lovelace"},
                        "last_ts": None,
                        "incoming_employments": [
                            {
                                "i": 0,
                                "unit_uid": "unit-uid",
                                "matched": False,
                                "props": None,
                                "last_ts": None,
                            }
                        ],
                    }
                }
            ]),
            FakeAsyncResult(),
        ])

        await officer.upsert_officer_batch(
            tx,
            [
                {
                    "data": {
                        "first_name": "Ada",
                        "last_name": "Byron",
                        "state_ids": [{"state": "NY", "id_name": "Tax ID", "value": "123"}],
                    },
                    "employment": [
                        {
                            "agency_label": "Albany PD",
                            "a_hq_state": "NY",
                            "unit_label": "Patrol",
                            "u_hq_state": "NY",
                            "highest_rank": "Officer",
                            "status": "active",
                        }
                    ],
                    "source_uid": "src",
                    "url": "https://example.test",
                    "scraped_at": SCRAPED_AT,
                }
            ],
            NullLog(),
        )

        rows = tx.calls[1]["params"]["rows"]
        self.assertEqual(len(rows[0]["employments"]), 1)
        self.assertIsNone(rows[0]["employments"][0]["diff"])


if __name__ == "__main__":
    unittest.main()
