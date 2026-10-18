from playwright.sync_api import expect

from e2e.browser_doubles import fulfill_sse, record_toasts, toasts


def test_toasts_use_semantic_colors_and_accessibility_roles(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        """() => {
          window.setTheme('vireo-gold');
          showToast('Saved', 'success');
          showToast('For your information', 'info');
          showToast('Needs attention', 'warning');
          showToast('Failed', 'error');
          showToast('Neutral default');
        }"""
    )

    expected = {
        "success": (
            "var(--success)", "rgb(46, 125, 50)", "rgb(255, 255, 255)",
            "status", "polite",
        ),
        "info": (
            "var(--info)", "rgb(79, 109, 122)", "rgb(255, 255, 255)",
            "status", "polite",
        ),
        "warning": (
            "var(--warning)", "rgb(200, 122, 0)", "rgb(0, 0, 0)",
            "status", "polite",
        ),
        "error": (
            "var(--danger)", "rgb(192, 57, 43)", "rgb(255, 255, 255)",
            "alert", "assertive",
        ),
    }
    for kind, (token, background, foreground, role, live) in expected.items():
        toast = page.locator(f'#toastContainer > [data-type="{kind}"]').first
        expect(toast).to_be_visible()
        assert toast.evaluate("el => el.style.background") == token
        assert toast.evaluate("el => getComputedStyle(el).backgroundColor") == background
        assert toast.evaluate("el => getComputedStyle(el).color") == foreground
        assert toast.get_attribute("role") == role
        assert toast.get_attribute("aria-live") == live

    default_toast = page.locator("#toastContainer > div").last
    expect(default_toast).to_have_text("Neutral default")
    assert default_toast.get_attribute("data-type") == "info"
    assert default_toast.evaluate("el => el.style.background") == "var(--info)"


def test_develop_completion_toast_reads_nested_job_result(live_server, page):
    payloads = [
        {
            "status": "completed",
            "result": {"developed": 2, "errors": 0, "total": 2},
            "errors": [],
        },
        {
            "status": "failed",
            "result": {"developed": 1, "errors": 1, "total": 2},
            "errors": ["robin.jpg: export failed"],
        },
    ]
    page.route("**/api/darktable/status", lambda route: route.fulfill(json={"available": True}))
    page.route("**/api/jobs/develop", lambda route: route.fulfill(json={"job_id": 42}))
    page.route("**/api/jobs/42/stream", lambda route: fulfill_sse(route, "complete", payloads.pop(0)))
    page.goto(f"{live_server['url']}/browse")
    record_toasts(page)
    page.evaluate("window.confirm = () => true")

    page.evaluate("developPhotos([1, 2])")
    page.wait_for_function("window.__toasts.some(toast => toast.message.startsWith('Development'))")
    page.evaluate("developPhotos([1, 2])")
    page.wait_for_function(
        "window.__toasts.filter(toast => toast.message.startsWith('Development')).length === 2"
    )

    assert toasts(page, "Development") == [
        {
            "message": "Development complete: 2 developed, 0 errors",
            "type": "success",
        },
        {
            "message": "Development failed: 1 developed, 1 errors",
            "type": "error",
        },
    ]


def test_partial_path_copy_uses_warning_with_counts(live_server, page):
    page.goto(f"{live_server['url']}/browse")

    result = page.evaluate(
        """async () => {
          const original = {
            safeFetch: window.safeFetch,
            showToast: window.showToast,
            clipboard: Object.getOwnPropertyDescriptor(navigator, 'clipboard')
          };
          const seen = [];
          let copied = '';
          window.safeFetch = async url => {
            if (url.endsWith('/2')) throw new Error('missing');
            return {path: url.endsWith('/1') ? '/photos/one.jpg' : '/photos/three.jpg'};
          };
          window.showToast = (message, type) => seen.push({message, type});
          Object.defineProperty(navigator, 'clipboard', {
            configurable: true,
            value: {writeText: async text => { copied = text; }}
          });
          try {
            await pageModules.contextMenus.copyPhotoPaths([1, 2, 3]);
            return {copied, seen};
          } finally {
            window.safeFetch = original.safeFetch;
            window.showToast = original.showToast;
            if (original.clipboard) {
              Object.defineProperty(navigator, 'clipboard', original.clipboard);
            } else {
              delete navigator.clipboard;
            }
          }
        }"""
    )

    assert result == {
        "copied": "/photos/one.jpg\n/photos/three.jpg",
        "seen": [{
            "message": "2 of 3 paths copied; 1 could not be copied",
            "type": "warning",
        }],
    }
