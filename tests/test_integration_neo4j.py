from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import time
import unittest
import uuid
from pathlib import Path


RUN_NEO4J_TESTS = os.getenv("RUN_NEO4J_TESTS") == "1"
NEO4J_TEST_IMAGE = os.getenv("NEO4J_TEST_IMAGE", "neo4j:5.23-community")
NEO4J_TEST_PASSWORD = os.getenv("NEO4J_TEST_PASSWORD", "test-password-123")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@unittest.skipUnless(
    RUN_NEO4J_TESTS,
    "Set RUN_NEO4J_TESTS=1 to run Docker-backed Neo4j integration tests.",
)
class Neo4jLoaderIntegrationTests(unittest.IsolatedAsyncioTestCase):
    container_name: str
    bolt_port: int
    driver: object

    @classmethod
    def setUpClass(cls) -> None:
        try:
            import deepdiff  # noqa: F401
            import neomodel  # noqa: F401
            from neo4j import GraphDatabase
        except ImportError as exc:
            raise unittest.SkipTest(
                "Project Python dependencies are required for integration tests"
            ) from exc

        cls.GraphDatabase = GraphDatabase
        cls.container_name = f"npdc-loader-neo4j-test-{uuid.uuid4().hex[:12]}"
        cls.bolt_port = free_port()
        cls.http_port = free_port()

        try:
            subprocess.run(
                [
                    "docker",
                    "run",
                    "--detach",
                    "--rm",
                    "--name",
                    cls.container_name,
                    "--publish",
                    f"127.0.0.1:{cls.bolt_port}:7687",
                    "--publish",
                    f"127.0.0.1:{cls.http_port}:7474",
                    "--env",
                    f"NEO4J_AUTH=neo4j/{NEO4J_TEST_PASSWORD}",
                    NEO4J_TEST_IMAGE,
                ],
                check=True,
                stdout=subprocess.DEVNULL,
            )
        except FileNotFoundError as exc:
            raise unittest.SkipTest("Docker is required for integration tests") from exc

        cls.driver = cls.GraphDatabase.driver(
            f"bolt://127.0.0.1:{cls.bolt_port}",
            auth=("neo4j", NEO4J_TEST_PASSWORD),
        )
        deadline = time.monotonic() + 90
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                with cls.driver.session() as session:
                    session.run("RETURN 1").consume()
                return
            except Exception as exc:
                last_error = exc
                time.sleep(1)

        raise RuntimeError(f"Neo4j test container did not become ready: {last_error}")

    @classmethod
    def tearDownClass(cls) -> None:
        driver = getattr(cls, "driver", None)
        if driver is not None:
            driver.close()
        container_name = getattr(cls, "container_name", None)
        if container_name:
            subprocess.run(
                ["docker", "stop", container_name],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    async def asyncSetUp(self) -> None:
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n").consume()
            session.run(
                "CREATE (:Source {uid: $uid, name: $name, url: $url})",
                uid="src-test",
                name="Test Source",
                url="https://example.test/source",
            ).consume()

    async def test_loads_agency_into_neo4j_container(self):
        from neo4j import AsyncGraphDatabase

        from loader.cli import load_jsonl_to_neo4j

        row = {
            "model": "agency",
            "source_uid": "src-test",
            "url": "https://example.test/agencies/albany-pd",
            "scraped_at": "2024-01-02 03:04:05",
            "data": {
                "name": "Albany Police Department",
                "hq_state": "NY",
                "address": {
                    "street": "1 Main St",
                    "city": "Albany",
                    "state": "NY",
                    "postal_code": "12207",
                },
                "phone": "555-0100",
            },
        }

        tmp = tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8")
        with tmp:
            tmp.write(json.dumps(row))
            tmp.write("\n")

        async_driver = AsyncGraphDatabase.driver(
            f"bolt://127.0.0.1:{self.bolt_port}",
            auth=("neo4j", NEO4J_TEST_PASSWORD),
        )
        try:
            async with async_driver:
                await load_jsonl_to_neo4j(
                    tmp.name,
                    batch_size=1,
                    model_order=("agency",),
                    concurrency=1,
                    stop_on_error=True,
                    driver=async_driver,
                )
        finally:
            Path(tmp.name).unlink(missing_ok=True)

        with self.driver.session() as session:
            record = session.run(
                """
                MATCH (a:Agency {name: $name, hq_state: "NY"})
                MATCH (a)<-[:CHANGE_TO]-(:Change)-[:ATTRIBUTED_TO]->(:Source {uid: "src-test"})
                OPTIONAL MATCH (a)<-[:ESTABLISHED_BY]-(u:Unit {name: "Unknown", hq_state: "NY"})
                RETURN a.hq_address AS address, a.hq_city AS city, a.phone AS phone, u IS NOT NULL AS has_unknown_unit
                """,
                name="Albany Police Department",
            ).single()

        self.assertIsNotNone(record)
        self.assertEqual(record["address"], "1 Main St")
        self.assertEqual(record["city"], "Albany")
        self.assertEqual(record["phone"], "555-0100")
        self.assertTrue(record["has_unknown_unit"])


if __name__ == "__main__":
    unittest.main()
