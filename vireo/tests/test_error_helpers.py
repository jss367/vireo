def test_json_error_default_status(app_and_db):
    """json_error returns 400 by default."""
    app, _ = app_and_db
    client = app.test_client()
    # Hit an endpoint that returns json_error — workspace name required
    resp = client.post('/api/workspaces',
                       json={},
                       content_type='application/json')
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'error' in data


def test_json_error_custom_status(app_and_db):
    """json_error can return custom status codes."""
    app, _ = app_and_db
    client = app.test_client()
    # Hit an endpoint that returns 404
    resp = client.get('/api/photos/999999')
    assert resp.status_code == 404
    data = resp.get_json()
    assert 'error' in data
