"""Integration tests against the real Agent Relay API and PostgreSQL database.

This module validates Acceptance Scenario 1 from SPEC.md:
"Register two agents. One sends a task; the other claims and completes it; the sender reads the result."

The test runs against the real HTTP server and asserts state both via HTTP responses
and by querying the real PostgreSQL database directly.
"""

from __future__ import annotations

import os
import uuid
import httpx
import pytest
from sqlalchemy import create_engine, text

API_BASE_URL = os.getenv("RELAY_API_URL", "http://127.0.0.1:8000")
REAL_DB_URL = (
    os.getenv("REAL_DATABASE_URL")
    or os.getenv("DATABASE_URL")
    or "postgresql+psycopg://postgres:postgres@localhost:5432/agent_relay"
)
if REAL_DB_URL.startswith("postgresql://"):
    REAL_DB_URL = REAL_DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)


def is_server_available() -> bool:
    try:
        response = httpx.get(f"{API_BASE_URL}/", timeout=2.0)
        return response.status_code == 200
    except Exception:
        return False


def is_db_available() -> bool:
    try:
        engine = create_engine(REAL_DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def api_client():
    if not is_server_available():
        pytest.skip(f"Real Agent Relay API is not reachable at {API_BASE_URL}")
    with httpx.Client(base_url=API_BASE_URL, timeout=10.0) as client:
        yield client


@pytest.fixture(scope="module")
def db_engine():
    if not is_db_available():
        pytest.skip(f"Real database is not reachable at {REAL_DB_URL}")
    engine = create_engine(REAL_DB_URL)
    yield engine
    engine.dispose()


def test_acceptance_scenario_1_agent_task_exchange(api_client: httpx.Client, db_engine):
    """SPEC Scenario 1:

    1. Register two agents (sender and recipient).
    2. Sender sends a task to recipient.
    3. Recipient claims and completes the task.
    4. Sender reads the result.
    5. Verify state directly in the real database.
    """
    unique_run_id = uuid.uuid4().hex[:8]

    # --- Step 1: Register two agents ---
    sender_name = f"sender-agent-{unique_run_id}"
    sender_resp = api_client.post(
        "/api/v1/agents",
        json={"name": sender_name, "description": "Integration test sender agent"},
    )
    assert sender_resp.status_code == 201, f"Failed to register sender: {sender_resp.text}"
    sender_data = sender_resp.json()
    assert "agent_id" in sender_data
    assert "token" in sender_data
    sender_id = sender_data["agent_id"]
    sender_token = sender_data["token"]
    sender_headers = {"Authorization": f"Bearer {sender_token}"}

    recipient_name = f"worker-agent-{unique_run_id}"
    recipient_resp = api_client.post(
        "/api/v1/agents",
        json={"name": recipient_name, "description": "Integration test worker agent"},
    )
    assert recipient_resp.status_code == 201, f"Failed to register recipient: {recipient_resp.text}"
    recipient_data = recipient_resp.json()
    assert "agent_id" in recipient_data
    assert "token" in recipient_data
    recipient_id = recipient_data["agent_id"]
    recipient_token = recipient_data["token"]
    recipient_headers = {"Authorization": f"Bearer {recipient_token}"}

    # Verify both agents can access their own profile
    me_sender = api_client.get("/api/v1/agents/me", headers=sender_headers)
    assert me_sender.status_code == 200
    assert me_sender.json()["agent_id"] == sender_id

    me_recipient = api_client.get("/api/v1/agents/me", headers=recipient_headers)
    assert me_recipient.status_code == 200
    assert me_recipient.json()["agent_id"] == recipient_id

    # --- Step 2: Sender submits a task to Recipient ---
    task_input = f"Calculate prime factors for {unique_run_id}"
    idempotency_key = f"key-{unique_run_id}"
    task_resp = api_client.post(
        "/api/v1/tasks",
        headers={**sender_headers, "Idempotency-Key": idempotency_key},
        json={"to": recipient_id, "input": task_input},
    )
    assert task_resp.status_code == 201, f"Failed to submit task: {task_resp.text}"
    task_data = task_resp.json()
    assert "task_id" in task_data
    task_id = task_data["task_id"]
    assert task_data["status"] == "queued"

    # Verify task appears as queued when queried by sender
    initial_task = api_client.get(f"/api/v1/tasks/{task_id}", headers=sender_headers)
    assert initial_task.status_code == 200
    assert initial_task.json()["status"] == "queued"
    assert initial_task.json()["output"] is None

    # --- Step 3: Recipient worker claims the task ---
    claim_resp = api_client.post(
        "/api/v1/tasks/claim",
        headers=recipient_headers,
        json={"worker_id": f"worker-proc-{unique_run_id}", "wait_seconds": 2},
    )
    assert claim_resp.status_code == 200, f"Failed to claim task: {claim_resp.text}"
    claim_data = claim_resp.json()
    assert claim_data["task_id"] == task_id
    assert claim_data["from"] == sender_id
    assert claim_data["input"] == task_input
    assert claim_data["attempt"] == 1
    assert "claim_token" in claim_data
    claim_token = claim_data["claim_token"]

    # --- Step 4: Recipient completes the task ---
    expected_output = f"Processed factors successfully for {unique_run_id}: [2, 3, 7]"
    complete_resp = api_client.post(
        f"/api/v1/tasks/{task_id}/complete",
        headers=recipient_headers,
        json={"claim_token": claim_token, "output": expected_output},
    )
    assert complete_resp.status_code == 200, f"Failed to complete task: {complete_resp.text}"
    complete_data = complete_resp.json()
    assert complete_data["task_id"] == task_id
    assert complete_data["status"] == "completed"

    # --- Step 5: Sender retrieves task and reads result ---
    sender_read_resp = api_client.get(f"/api/v1/tasks/{task_id}", headers=sender_headers)
    assert sender_read_resp.status_code == 200
    task_result = sender_read_resp.json()
    assert task_result["task_id"] == task_id
    assert task_result["status"] == "completed"
    assert task_result["from"] == sender_id
    assert task_result["to"] == recipient_id
    assert task_result["input"] == task_input
    assert task_result["output"] == expected_output
    assert task_result["error"] is None
    assert task_result["attempt_count"] == 1
    assert task_result["finished_at"] is not None

    # Delivery history verification via API
    attempts_resp = api_client.get(f"/api/v1/tasks/{task_id}/attempts", headers=sender_headers)
    assert attempts_resp.status_code == 200
    attempts_data = attempts_resp.json()
    assert len(attempts_data["items"]) == 1
    attempt_record = attempts_data["items"][0]
    assert attempt_record["attempt"] == 1
    assert attempt_record["worker_id"] == f"worker-proc-{unique_run_id}"
    assert attempt_record["outcome"] == "completed"
    # Claim token should never be leaked in attempt history
    assert "claim_token" not in attempt_record

    # --- Step 6: Verify durable state in the real database ---
    with db_engine.connect() as conn:
        # Check sender and recipient exist in agents table
        db_agents = conn.execute(
            text("SELECT id, name FROM agents WHERE id IN (:sender_id, :recipient_id)"),
            {"sender_id": sender_id, "recipient_id": recipient_id},
        ).fetchall()
        db_agent_map = {row.id: row.name for row in db_agents}
        assert sender_id in db_agent_map
        assert db_agent_map[sender_id] == sender_name
        assert recipient_id in db_agent_map
        assert db_agent_map[recipient_id] == recipient_name

        # Check task in tasks table
        db_task = conn.execute(
            text("SELECT id, sender_id, recipient_id, status, input, output, attempt_count FROM tasks WHERE id = :task_id"),
            {"task_id": task_id},
        ).fetchone()
        assert db_task is not None
        assert db_task.sender_id == sender_id
        assert db_task.recipient_id == recipient_id
        assert db_task.status == "completed"
        assert db_task.input == task_input
        assert db_task.output == expected_output
        assert db_task.attempt_count == 1

        # Check attempt in attempts table
        db_attempt = conn.execute(
            text("SELECT task_id, attempt_number, worker_id, outcome FROM attempts WHERE task_id = :task_id"),
            {"task_id": task_id},
        ).fetchone()
        assert db_attempt is not None
        assert db_attempt.attempt_number == 1
        assert db_attempt.worker_id == f"worker-proc-{unique_run_id}"
        assert db_attempt.outcome == "completed"
