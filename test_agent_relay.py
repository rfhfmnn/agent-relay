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
_scratch_db = (Path(tempfile.gettempdir()) / "agent-relay-test.db").as_posix()
os.environ.setdefault("RELAY_DATABASE_URL", f"sqlite:///{_scratch_db}")

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

import main
from database import Agent, Attempt, Base, Task, as_db_time, db_session, engine, utcnow
from storage import claim_one


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


def test_acceptance_scenario_1_register_send_claim_complete_read():
    """Scenario 1: Register two agents. One sends a task; the other claims and completes it; the sender reads the result.

    Verifies the complete flow end-to-end against the real API routes and the real database state.
    """
    with TestClient(main.app) as client:
        # Step 1: Register two agents (Alice and Bob)
        alice_data, alice_headers = register(client, "alice")
        bob_data, bob_headers = register(client, "bob")

        alice_id = alice_data["agent_id"]
        bob_id = bob_data["agent_id"]
        assert alice_id.startswith("agent_")
        assert bob_id.startswith("agent_")

        # Verify DB directly: both agents exist, token is hashed rather than stored plaintext
        with db_session() as db:
            db_alice = db.get(Agent, alice_id)
            assert db_alice is not None
            assert db_alice.name == "alice"
            assert db_alice.token_hash != alice_data["token"]
            assert len(db_alice.token_hash) == 64

            db_bob = db.get(Agent, bob_id)
            assert db_bob is not None
            assert db_bob.name == "bob"
            assert db_bob.token_hash != bob_data["token"]

        # Step 2: Alice sends a task to Bob
        task_input = "Please review this Python function: def add(a, b): return a + b"
        send_response = client.post(
            "/api/v1/tasks",
            headers=alice_headers,
            json={"to": bob_id, "input": task_input},
        )
        assert send_response.status_code == 201
        task_data = send_response.json()
        task_id = task_data["task_id"]
        assert task_data["status"] == "queued"

        # Verify DB directly: task is persisted in 'queued' state with no attempts
        with db_session() as db:
            db_task = db.get(Task, task_id)
            assert db_task is not None
            assert db_task.sender_id == alice_id
            assert db_task.recipient_id == bob_id
            assert db_task.input == task_input
            assert db_task.status == "queued"
            assert db_task.attempt_count == 0
            assert db_task.output is None
            assert db_task.finished_at is None
            assert len(db_task.attempts) == 0

        # Step 3: Bob claims and completes the task
        claim_response = client.post(
            "/api/v1/tasks/claim",
            headers=bob_headers,
            json={"worker_id": "bob-worker-1", "wait_seconds": 0},
        )
        assert claim_response.status_code == 200
        claim_data = claim_response.json()
        assert claim_data["task_id"] == task_id
        assert claim_data["from"] == alice_id
        assert claim_data["input"] == task_input
        assert claim_data["attempt"] == 1
        assert "claim_token" in claim_data
        assert "lease_expires_at" in claim_data
        claim_token = claim_data["claim_token"]

        # Verify DB directly: task is 'processing', attempt count incremented, and attempt record created
        with db_session() as db:
            db_task = db.get(Task, task_id)
            assert db_task.status == "processing"
            assert db_task.attempt_count == 1
            attempts = list(db_task.attempts)
            assert len(attempts) == 1
            assert attempts[0].attempt_number == 1
            assert attempts[0].worker_id == "bob-worker-1"
            assert attempts[0].outcome == "processing"
            assert attempts[0].claim_token_hash != claim_token
            assert attempts[0].finished_at is None

        # Bob completes the task
        task_output = "The function is correct and has no issues."
        complete_response = client.post(
            f"/api/v1/tasks/{task_id}/complete",
            headers=bob_headers,
            json={"claim_token": claim_token, "output": task_output},
        )
        assert complete_response.status_code == 200
        assert complete_response.json()["task_id"] == task_id
        assert complete_response.json()["status"] == "completed"

        # Verify DB directly: task is 'completed', finished_at set, output recorded, attempt marked 'completed'
        with db_session() as db:
            db_task = db.get(Task, task_id)
            assert db_task.status == "completed"
            assert db_task.output == task_output
            assert db_task.error is None
            assert db_task.finished_at is not None
            attempts = list(db_task.attempts)
            assert len(attempts) == 1
            assert attempts[0].outcome == "completed"
            assert attempts[0].finished_at is not None

        # Step 4: Alice (sender) reads the result
        read_response = client.get(f"/api/v1/tasks/{task_id}", headers=alice_headers)
        assert read_response.status_code == 200
        result = read_response.json()
        assert result["task_id"] == task_id
        assert result["from"] == alice_id
        assert result["to"] == bob_id
        assert result["input"] == task_input
        assert result["status"] == "completed"
        assert result["output"] == task_output
        assert result["error"] is None
        assert result["attempt_count"] == 1
        assert result["created_at"] is not None
        assert result["finished_at"] is not None

        # Alice also inspects the attempt history via the API
        attempts_response = client.get(f"/api/v1/tasks/{task_id}/attempts", headers=alice_headers)
        assert attempts_response.status_code == 200
        attempts_list = attempts_response.json()["items"]
        assert len(attempts_list) == 1
        assert attempts_list[0]["attempt"] == 1
        assert attempts_list[0]["worker_id"] == "bob-worker-1"
        assert attempts_list[0]["outcome"] == "completed"
        assert "claim_token" not in attempts_list[0]


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
