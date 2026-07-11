def test_browser_transport_authenticates_internal_mutations(live_server, page):
    """The real browser client establishes a session and adds its unsafe header."""
    live_server["app"].config["BROWSER_AUTH_ENABLED"] = True
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]

    page.goto(f"{url}/browse")
    cookies = page.context.cookies(url)
    session = next(cookie for cookie in cookies if cookie["name"] == "vireo_session")
    assert session["httpOnly"] is True
    assert session["sameSite"] == "Strict"

    result = page.evaluate(
        """async ({photoId}) => {
          const blocked = await Vireo.api.nativeFetch('/api/photos/' + photoId + '/rating', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({rating: 4})
          });
          const allowed = await Vireo.api.fetch('/api/photos/' + photoId + '/rating', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({rating: 4})
          });
          return {blocked: blocked.status, allowed: allowed.status};
        }""",
        {"photoId": photo_id},
    )

    assert result == {"blocked": 403, "allowed": 200}
