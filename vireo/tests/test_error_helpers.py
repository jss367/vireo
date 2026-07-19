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
    assert data['error'] == 'not found'
    assert data['code'] == 'not_found'
    assert 'no longer available in the active workspace' in data['message']


def test_unhandled_api_error_has_recovery_message_and_request_id(app_and_db):
    """Unexpected failures give the browser a safe recovery message and trace id."""
    app, _ = app_and_db

    @app.get('/api/test-unhandled-error')
    def _test_unhandled_error():
        raise RuntimeError('sensitive internal detail')

    resp = app.test_client().get('/api/test-unhandled-error')
    assert resp.status_code == 500
    data = resp.get_json()
    assert data['error'] == 'Internal server error'
    assert data['code'] == 'internal_error'
    assert data['message'] == 'Something went wrong in Vireo. Try again.'
    assert data['request_id'] == resp.headers['X-Request-ID']
    assert 'sensitive internal detail' not in data['message']
