"""Controlled geometry for browser tests that exercise synthetic zoom scenarios."""


def install_viewport_test_helpers(page):
    page.add_init_script('''(() => {
      let factory;
      Object.defineProperty(window, 'VireoLightboxViewport', {
        get: () => factory,
        set: value => {
          factory = {create(options) {
            const browser = {
              document, ResizeObserver: window.ResizeObserver,
              get devicePixelRatio() { return window.viewportPixelRatioForTest ?? window.devicePixelRatio; },
              setTimeout: window.setTimeout.bind(window), clearTimeout: window.clearTimeout.bind(window),
              addEventListener: window.addEventListener.bind(window),
              removeEventListener: window.removeEventListener.bind(window)
            };
            window.viewportPhotoForTest = options.photo;
            return value.create({...options, window: browser,
              photo: () => ({...options.photo(), ...window.viewportGeometryForTest})});
          }};
        }
      });
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
    })()''')
