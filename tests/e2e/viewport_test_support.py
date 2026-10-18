"""Controlled geometry for browser tests that exercise synthetic zoom scenarios.

The lightbox builds its viewport controller when lightbox/state.js loads.
After every navigation (``page.after_navigation``, run by the ``page``
fixture once the navbar's modules are exposed; see navbar_modules.py) this
replaces it with one built from the same host options, except that pixel
ratio and photo geometry come from the test. The controller attaches its
listeners when the lightbox opens, so swapping it before then is safe.
"""

_HELPERS = '''(() => {
  window.setViewportNativeZoomForTest = zoom => {
    const photo = viewportPhotoForTest();
    const img = document.getElementById('lightboxImg');
    // Give the injected geometry provider dimensions even when the fixture
    // catalog deliberately has none. Pixel ratio controls the 1:1 stop.
    window.viewportGeometryForTest = {...window.viewportGeometryForTest,
      width: photo.width || img.naturalWidth || 100,
      height: photo.height || img.naturalHeight || 100};
    vireoLightboxViewport.layoutMetrics();
    window.viewportPixelRatioForTest = 1 / (vireoLightboxViewport.fitScale() * zoom);
    vireoLightboxViewport.recomputeNativeZoom();
  };
  window.setViewportFitScaleForTest = scale => {
    const wrap = document.getElementById('lightboxWrap');
    window.viewportGeometryForTest = {
      width: wrap.clientWidth / scale, height: wrap.clientHeight / scale,
      orientation: null, recipe: null, pairKnown: false
    };
    vireoLightboxViewport.layoutMetrics();
  };
  window.saveViewportForTest = (photoId, state) => {
    if (vireoLightboxSession.requestedPhotoId() == null) {
      vireoLightboxViewport.beginPhoto(photoId, {fallbackViewportState: state});
      vireoLightboxViewport.save(photoId);
      vireoLightboxViewport.close();
    } else {
      const current = vireoLightboxViewport.currentView();
      vireoLightboxViewport.applyView(state);
      vireoLightboxViewport.save(photoId);
      vireoLightboxViewport.applyView(current);
    }
  };
})()'''

_REPLACE_VIEWPORT = '''() => {
  const host = lbViewportHost;
  const browser = {
    document, ResizeObserver: window.ResizeObserver,
    get devicePixelRatio() { return window.viewportPixelRatioForTest ?? window.devicePixelRatio; },
    setTimeout: window.setTimeout.bind(window), clearTimeout: window.clearTimeout.bind(window),
    addEventListener: window.addEventListener.bind(window),
    removeEventListener: window.removeEventListener.bind(window)
  };
  // Call through the host on every use, as the lightbox's own controller
  // does, so a test that replaces one of its functions still reaches it.
  const options = Object.fromEntries(Object.keys(host).map(key => [key, (...args) => host[key](...args)]));
  window.viewportPhotoForTest = () => host.photo();
  // The exposure's setter installs it through replaceLightboxControllers.
  window.vireoLightboxViewport = VireoLightboxViewport.create({...options, window: browser,
    photo: () => ({...host.photo(), ...window.viewportGeometryForTest})});
}'''


def _replace_viewport(page):
    page.evaluate(_REPLACE_VIEWPORT)


def install_viewport_test_helpers(page):
    page.add_init_script(_HELPERS)
    page.after_navigation.append(_replace_viewport)
