"""Behavior snapshots for stable statuses and JSON error envelopes."""

import json
from pathlib import Path

import pytest

CONTRACT_PATH = Path(__file__).with_name("contracts") / "api_responses.json"
CASES = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", CASES, ids=[case["name"] for case in CASES])
def test_api_response_contract(app_and_db, case):
    app, _ = app_and_db
    headers = {
        key: app.config["API_TOKEN"] if value == "$api_token" else value
        for key, value in case.get("headers", {}).items()
    }

    response = app.test_client().open(
        case["path"],
        method=case["method"],
        headers=headers,
        json=case.get("json"),
    )

    assert response.status_code == case["status"]
    request_id = response.headers.get("X-Request-ID")
    assert request_id

    body = response.get_json()
    if "error" not in case:
        assert body == case["body"]
        return

    assert set(body) == {"error", "code", "request_id"}
    assert body["request_id"] == request_id
    assert {"error": body["error"], "code": body["code"]} == case["error"]
