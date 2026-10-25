"""Invalid rating requests cannot silently clear ratings or reach persistence."""

import json

import pytest
from flask import Flask
from web.photo_review import create_photo_review_blueprint
from web.responses import json_error


@pytest.fixture
def client():
    app = Flask(__name__)
    app.config["TESTING"] = True

    def unexpected_db():
        pytest.fail("invalid input reached the database")

    app.register_blueprint(create_photo_review_blueprint(unexpected_db, json_error))
    return app.test_client()


@pytest.mark.parametrize("path", ["/api/photos/1/rating", "/api/batch/rating"])
@pytest.mark.parametrize("body", [None, False, 1, "rating", [], [1]])
def test_rating_requires_object_json(client, path, body):
    response = client.post(path, data=json.dumps(body), content_type="application/json")
    assert response.status_code == 400
    assert response.json["error"] == "request body must be a JSON object"


@pytest.mark.parametrize("path", ["/api/photos/1/rating", "/api/batch/rating"])
@pytest.mark.parametrize("body", ['{"rating":', ""])
def test_rating_rejects_missing_or_malformed_json(client, path, body):
    assert client.post(path, data=body, content_type="application/json").status_code == 400


@pytest.mark.parametrize("path", ["/api/photos/1/rating", "/api/batch/rating"])
@pytest.mark.parametrize("rating", [None, True, False, 1.5, "3", -1, 6, [], {}])
def test_rating_requires_explicit_integer_in_range(client, path, rating):
    assert client.post(path, json={"rating": rating, "photo_ids": [1]}).status_code == 400


@pytest.mark.parametrize("path", ["/api/photos/1/rating", "/api/batch/rating"])
def test_missing_rating_does_not_default_to_zero(client, path):
    assert client.post(path, json={"photo_ids": [1]}).status_code == 400


@pytest.mark.parametrize("ids", [None, [], "1", 1, {"id": 1}, [True], [1.0], ["1"], [0], [-1], [2**63], [1, False]])
def test_batch_rating_requires_valid_photo_ids(client, ids):
    assert client.post("/api/batch/rating", json={"rating": 3, "photo_ids": ids}).status_code == 400


@pytest.mark.parametrize("pid", [0, 2**63, 2**64])
def test_single_rating_requires_valid_photo_id(client, pid):
    assert client.post(f"/api/photos/{pid}/rating", json={"rating": 3}).status_code == 400


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("rating", [0, 5])
def test_explicit_rating_boundaries_are_saved(app_and_db, batch, rating):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    path = "/api/batch/rating" if batch else f"/api/photos/{pid}/rating"
    response = app.test_client().post(path, json={"rating": rating, "photo_ids": [pid]})
    assert response.status_code == 200
    assert db.photos.get(pid)["rating"] == rating
