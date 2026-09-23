"""Protocol tests for the SQLite starter.

These tests intentionally exercise storage calls from multiple threads: that
is the closest local equivalent to several worker processes racing to claim an
inbox.  The production guarantee comes from SQLite's BEGIN IMMEDIATE boundary,
not from a Python lock.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

# Default to a scratch DB so `pytest` never resets the dev server's
# `./agent-relay.db`. Respect an explicit RELAY_DATABASE_URL/DATABASE_URL
# (e.g. CI pointing at PostgreSQL), but otherwise isolate tests.
_test_db_path = (Path(tempfile.gettempdir()) / "agent-relay-test.db").as_posix()
os.environ.setdefault("RELAY_DATABASE_URL", f"sqlite:///{_test_db_path}")

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

import main
from database import Agent, Attempt, Base, Task, as_db_time, db_session, engine, utcnow
from storage import claim_one, secret_hash



@pytest.fixture(autouse=True)
def empty_database():
    # Resets whatever DB RELAY_DATABASE_URL points at. Defaults to the
    # scratch /tmp file above; never run against a DB with data you need.
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


def register(client: TestClient, name: str) -> tuple[dict, dict[str, str]]:
    response = client.post("/api/v1/agents", json={"name": name})
    assert response.status_code == 201
    data = response.json()
    return data, {"Authorization": f"Bearer {data['token']}"}


def test_protocol_idempotency_terminal_retry_and_auth_boundary():
    with TestClient(main.app) as client:
        sender, sender_headers = register(client, "sender")
        recipient, recipient_headers = register(client, "uppercase")
        sent = client.post(
            "/api/v1/tasks",
            headers={**sender_headers, "Idempotency-Key": "demo-1"},
            json={"to": recipient["agent_id"], "input": "hello relay"},
        )
        assert sent.status_code == 201
        duplicate = client.post(
            "/api/v1/tasks",
            headers={**sender_headers, "Idempotency-Key": "demo-1"},
            json={"to": recipient["agent_id"], "input": "hello relay"},
        )
        assert duplicate.status_code == 201
        assert duplicate.json() == sent.json()
        conflict = client.post(
            "/api/v1/tasks",
            headers={**sender_headers, "Idempotency-Key": "demo-1"},
            json={"to": recipient["agent_id"], "input": "different"},
        )
        assert conflict.status_code == 409

        task_id = sent.json()["task_id"]
        claim = client.post(
            "/api/v1/tasks/claim",
            headers=recipient_headers,
            json={"worker_id": "worker-a", "wait_seconds": 0},
        )
        assert claim.status_code == 200
        claim_data = claim.json()
        assert "claim_token" in claim_data
        complete = client.post(
            f"/api/v1/tasks/{task_id}/complete",
            headers=recipient_headers,
            json={"claim_token": claim_data["claim_token"], "output": "HELLO RELAY"},
        )
        assert complete.status_code == 200
        retry = client.post(
            f"/api/v1/tasks/{task_id}/complete",
            headers=recipient_headers,
            json={"claim_token": claim_data["claim_token"], "output": "HELLO RELAY"},
        )
        assert retry.status_code == 200
        assert client.get(f"/api/v1/tasks/{task_id}", headers=recipient_headers).status_code == 200
        forbidden = client.get(f"/api/v1/tasks/{task_id}", headers={"Authorization": f"Bearer {sender['token']}"})
        assert forbidden.status_code == 200  # sender is an authorized participant
        no_credentials = client.get("/api/v1/agents")
        assert no_credentials.status_code == 401
        attempts = client.get(f"/api/v1/tasks/{task_id}/attempts", headers=sender_headers).json()
        assert attempts["items"][0]["outcome"] == "completed"
        assert "claim_token" not in attempts["items"][0]


def test_sqlite_atomic_claims_distribute_without_overlap():
    with TestClient(main.app) as client:
        _sender, sender_headers = register(client, "sender")
        recipient, _recipient_headers = register(client, "recipient")
        for index in range(16):
            response = client.post(
                "/api/v1/tasks",
                headers=sender_headers,
                json={"to": recipient["agent_id"], "input": f"task-{index}"},
            )
            assert response.status_code == 201
        with ThreadPoolExecutor(max_workers=16) as pool:
            claims = list(pool.map(lambda index: claim_one(recipient["agent_id"], f"worker-{index}"), range(16)))
        claims = [claim for claim in claims if claim is not None]
        assert len(claims) == 16
        assert len({claim["task_id"] for claim in claims}) == 16
        with db_session() as db:
            processing = list(db.query(Task).filter(Task.status == "processing"))
            assert len(processing) == 16
            assert all(task.attempt_count == 1 for task in processing)


def test_expiry_requeues_and_old_token_is_stale_before_recovery():
    with TestClient(main.app) as client:
        _sender, sender_headers = register(client, "sender")
        recipient, recipient_headers = register(client, "recipient")
        task = client.post(
            "/api/v1/tasks",
            headers=sender_headers,
            json={"to": recipient["agent_id"], "input": "recover me"},
        ).json()
        task_id = task["task_id"]
        first = client.post(
            "/api/v1/tasks/claim", headers=recipient_headers, json={"worker_id": "dead", "wait_seconds": 0}
        ).json()
        with db_session() as db:
            attempt = db.query(Attempt).filter(Attempt.task_id == task_id).one()
            attempt.lease_expires_at = as_db_time(utcnow() - timedelta(seconds=1))
        stale = client.post(
            f"/api/v1/tasks/{task_id}/complete",
            headers=recipient_headers,
            json={"claim_token": first["claim_token"], "output": "TOO LATE"},
        )
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "stale_claim"
        assert main.recover_expired() == 1
        second = client.post(
            "/api/v1/tasks/claim", headers=recipient_headers, json={"worker_id": "replacement", "wait_seconds": 0}
        )
        assert second.status_code == 200
        assert second.json()["attempt"] == 2
        assert second.json()["claim_token"] != first["claim_token"]


def test_dashboard_is_asset_and_invalid_input_is_documented_error():
    with TestClient(main.app) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "sessionStorage" in page.text
        missing_name = client.post("/api/v1/agents", json={})
        assert missing_name.status_code == 400
        assert missing_name.json()["error"]["code"] == "invalid_input"


def test_scenario_1_register_send_claim_complete_read():
    """Acceptance Scenario 1:
    Register two agents. One sends a task; the other claims and completes it;
    the sender reads the result.

    Validates end-to-end HTTP API behavior and persistent database state at each step.
    """
    with TestClient(main.app) as client:
        # Step 1: Register two agents (Alice and Bob)
        alice_res = client.post(
            "/api/v1/agents", json={"name": "alice", "description": "Task sender agent"}
        )
        assert alice_res.status_code == 201
        alice = alice_res.json()
        assert alice["agent_id"].startswith("agent_")
        assert alice["token"].startswith("agt_")
        alice_headers = {"Authorization": f"Bearer {alice['token']}"}

        bob_res = client.post(
            "/api/v1/agents", json={"name": "bob", "description": "Task recipient worker"}
        )
        assert bob_res.status_code == 201
        bob = bob_res.json()
        assert bob["agent_id"].startswith("agent_")
        assert bob["token"].startswith("agt_")
        bob_headers = {"Authorization": f"Bearer {bob['token']}"}

        # Verify DB state after Step 1
        with db_session() as db:
            alice_row = db.get(Agent, alice["agent_id"])
            assert alice_row is not None
            assert alice_row.name == "alice"
            assert alice_row.description == "Task sender agent"
            assert alice_row.token_hash == secret_hash(alice["token"])

            bob_row = db.get(Agent, bob["agent_id"])
            assert bob_row is not None
            assert bob_row.name == "bob"
            assert bob_row.description == "Task recipient worker"
            assert bob_row.token_hash == secret_hash(bob["token"])

        # Step 2: Alice sends a task to Bob
        task_payload = {
            "to": bob["agent_id"],
            "input": "Summarize the findings of the quarterly report",
        }
        task_res = client.post("/api/v1/tasks", json=task_payload, headers=alice_headers)
        assert task_res.status_code == 201
        task = task_res.json()
        task_id = task["task_id"]
        assert task_id.startswith("task_")
        assert task["status"] == "queued"

        # Verify DB state after Step 2
        with db_session() as db:
            task_row = db.get(Task, task_id)
            assert task_row is not None
            assert task_row.sender_id == alice["agent_id"]
            assert task_row.recipient_id == bob["agent_id"]
            assert task_row.input == "Summarize the findings of the quarterly report"
            assert task_row.status == "queued"
            assert task_row.output is None
            assert task_row.error is None
            assert task_row.attempt_count == 0

        # Step 3: Bob claims the task
        claim_res = client.post(
            "/api/v1/tasks/claim",
            json={"worker_id": "bob-worker-1", "wait_seconds": 5},
            headers=bob_headers,
        )
        assert claim_res.status_code == 200
        claim = claim_res.json()
        assert claim["task_id"] == task_id
        assert claim["attempt"] == 1
        assert claim["input"] == "Summarize the findings of the quarterly report"
        claim_token = claim["claim_token"]
        assert claim_token.startswith("clm_")
        assert claim["lease_expires_at"] is not None

        # Verify DB state after Step 3
        with db_session() as db:
            task_row = db.get(Task, task_id)
            assert task_row is not None
            assert task_row.status == "processing"
            assert task_row.attempt_count == 1

            attempt_row = (
                db.query(Attempt)
                .filter(Attempt.task_id == task_id, Attempt.attempt_number == 1)
                .first()
            )
            assert attempt_row is not None
            assert attempt_row.worker_id == "bob-worker-1"
            assert attempt_row.claim_token_hash == secret_hash(claim_token)
            assert attempt_row.outcome == "processing"
            assert attempt_row.finished_at is None

        # Step 4: Bob completes the task
        complete_payload = {
            "claim_token": claim_token,
            "output": "Report summary: Revenue up by 15%, costs decreased by 5%.",
        }
        comp_res = client.post(
            f"/api/v1/tasks/{task_id}/complete", json=complete_payload, headers=bob_headers
        )
        assert comp_res.status_code == 200
        comp_data = comp_res.json()
        assert comp_data["task_id"] == task_id
        assert comp_data["status"] == "completed"

        # Verify DB state after Step 4
        with db_session() as db:
            task_row = db.get(Task, task_id)
            assert task_row is not None
            assert task_row.status == "completed"
            assert (
                task_row.output
                == "Report summary: Revenue up by 15%, costs decreased by 5%."
            )
            assert task_row.finished_at is not None

            attempt_row = (
                db.query(Attempt)
                .filter(Attempt.task_id == task_id, Attempt.attempt_number == 1)
                .first()
            )
            assert attempt_row is not None
            assert attempt_row.outcome == "completed"
            assert attempt_row.terminal_action == "complete"
            assert attempt_row.finished_at is not None

        # Step 5: Alice (sender) reads the result
        get_res = client.get(f"/api/v1/tasks/{task_id}", headers=alice_headers)
        assert get_res.status_code == 200
        result = get_res.json()
        assert result["task_id"] == task_id
        assert result["from"] == alice["agent_id"]
        assert result["to"] == bob["agent_id"]
        assert result["input"] == "Summarize the findings of the quarterly report"
        assert result["status"] == "completed"
        assert (
            result["output"]
            == "Report summary: Revenue up by 15%, costs decreased by 5%."
        )
        assert result["error"] is None
        assert result["attempt_count"] == 1
        assert result["created_at"] is not None
        assert result["finished_at"] is not None

        # Recipient (Bob) can also read the completed task
        bob_get_res = client.get(f"/api/v1/tasks/{task_id}", headers=bob_headers)
        assert bob_get_res.status_code == 200
        assert bob_get_res.json()["output"] == result["output"]

        # Attempts history check
        attempts_res = client.get(
            f"/api/v1/tasks/{task_id}/attempts", headers=alice_headers
        )
        assert attempts_res.status_code == 200
        attempts_data = attempts_res.json()
        assert len(attempts_data["items"]) == 1
        assert attempts_data["items"][0]["worker_id"] == "bob-worker-1"
        assert attempts_data["items"][0]["outcome"] == "completed"
        assert "claim_token" not in attempts_data["items"][0]

