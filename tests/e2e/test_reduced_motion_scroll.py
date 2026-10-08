"""Scripted scrolling honors live motion preferences on each affected page."""
import pytest


@pytest.mark.parametrize('surface', ['browse', 'import', 'settings', 'pipeline'])
def test_scripted_scroll_uses_current_motion_preference(live_server, page, surface):
    page.goto(f"{live_server['url']}/{surface}")
    page.wait_for_function("typeof preferredScrollBehavior === 'function'")
    page.evaluate('''() => {
      window.motionScrollCalls = [];
      const original = Element.prototype.scrollIntoView;
      Element.prototype.scrollIntoView = function(options) {
        motionScrollCalls.push({id: this.id, card: this.classList.contains('grid-card'), ...options});
        return original.call(this, options);
      };
    }''')
    if surface == 'browse':
        page.wait_for_selector('.grid-card')
    elif surface == 'import':
        # Exercise the retry response without starting a filesystem job.
        page.evaluate('''() => {
          retryBodyFromFinishedJob = () => ({sources: ['/example'], destination: '/archive'});
          updateStartGate = () => {};
          watchJob = () => {};
          const originalFetch = window.fetch;
          window.fetch = (url, ...args) => url === '/api/jobs/import-photos'
            ? Promise.resolve({ok: true, json: async () => ({job_id: 'motion-test'})})
            : originalFetch(url, ...args);
        }''')
    elif surface == 'settings':
        # Keep the real save-success scroll path while isolating persistence.
        page.evaluate('''() => {
          _nwAssembledTarget = () => ({id: 'motion-test'});
          renderRemoteTargets = () => {};
          _nwSetNext = () => {};
          _saveConfigNow = async () => {};
          closeNasWizard = () => {};
          if (!document.getElementById('nwBack')) {
            const back = document.createElement('button'); back.id = 'nwBack'; document.body.append(back);
          }
        }''')
    for preference, behavior in [('no-preference', 'smooth'), ('reduce', 'auto'), ('no-preference', 'smooth')]:
        page.emulate_media(reduced_motion=preference)
        page.evaluate('motionScrollCalls = []')
        if surface == 'browse':
            page.evaluate('scrollToCard(0)')
            calls = page.evaluate('motionScrollCalls')
            assert len(calls) == 1 and calls[0]['card']
            assert calls[0]['block'] == 'nearest'
        elif surface == 'import':
            page.evaluate("showError('Example validation failure', document.getElementById('destInput'))")
            page.evaluate("useStagingAsImportSource({source_root: '/example'})")
            page.evaluate('retryFailedImport()')
            calls = page.evaluate('motionScrollCalls')
            assert [(c['id'], c['block']) for c in calls] == [
                ('destInput', 'center'), ('sourceCard', 'start'), ('progressCard', 'center')]
        elif surface == 'settings':
            page.evaluate('nwSave()')
            calls = page.evaluate('motionScrollCalls')
            assert len(calls) == 1 and calls[0]['id'] == 'cfgRemoteTargetsList'
            assert calls[0]['block'] == 'center'
        else:
            page.evaluate("_showPipelineError(['Example pipeline failure'])")
            calls = page.evaluate('motionScrollCalls')
            assert len(calls) == 1 and calls[0]['id'] == 'pipelineErrorBanner'
            assert calls[0]['block'] == 'nearest'
        assert all(c['behavior'] == behavior for c in calls)


def test_browse_deep_link_uses_current_motion_preference(live_server, page):
    photo_id = live_server['data']['photos'][0]
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_selector('.grid-card')
    page.evaluate('''() => {
      window.deepLinkScrollCalls = [];
      const original = Element.prototype.scrollIntoView;
      Element.prototype.scrollIntoView = function(options) {
        if (this.classList.contains('grid-card')) deepLinkScrollCalls.push(options);
        return original.call(this, options);
      };
    }''')
    for preference, behavior in [('no-preference', 'smooth'), ('reduce', 'auto'), ('no-preference', 'smooth')]:
        page.emulate_media(reduced_motion=preference)
        page.evaluate('deepLinkScrollCalls = []')
        page.evaluate('id => _runPhotoDeepLink(id)', photo_id)
        assert page.evaluate('deepLinkScrollCalls') == [{'block': 'center', 'behavior': behavior}]
