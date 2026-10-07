"""Pinned real Leaflet must track map sizes changed by the bottom panel."""

import re
from pathlib import Path

from playwright.sync_api import expect

VENDOR = Path(__file__).parent / 'vendor'


def test_map_size_tracks_panel_toggle_and_drag(live_server, page):
    db = live_server['db']
    db.conn.execute('UPDATE photos SET latitude=37+id*2, longitude=-122+id*2')
    db.conn.commit()
    page.set_viewport_size({'width': 1100, 'height': 800})
    page.add_init_script("localStorage.setItem('vireo_panel_open', 'false'); localStorage.setItem('vireo_panel_height', '240');")

    def dependency(route):
        url = route.request.url
        if url.endswith('/leaflet.js'):
            route.fulfill(path=str(VENDOR / 'leaflet-1.9.4.js'), content_type='application/javascript')
        elif url.endswith('/leaflet.css'):
            route.fulfill(path=str(VENDOR / 'leaflet-1.9.4.css'), content_type='text/css')
        elif url.endswith('.js'):
            # Clustering is immaterial to container sizing. Keep real
            # Leaflet layers, projection, fitBounds and size caching.
            route.fulfill(body='L.markerClusterGroup = function() { const group = L.featureGroup(); group.addLayers = layers => { layers.forEach(layer => group.addLayer(layer)); return group; }; return group; };',
                          content_type='application/javascript')
        else:
            route.fulfill(body='', content_type='text/css')

    page.route('https://unpkg.com/**', dependency)
    page.route('**/*.png', lambda route: route.abort())
    page.goto(f"{live_server['url']}/map")
    page.wait_for_function('window.map && markers.getLayers().length > 1')
    assert page.evaluate('L.version') == '1.9.4'

    def assert_current_size_and_fit():
        page.wait_for_function('!map._animatingZoom && !(map._panAnim && map._panAnim._inProgress)')
        page.wait_for_function('''() => {
          const element = map.getContainer();
          const size = map.getSize();
          return size.x === element.clientWidth && size.y === element.clientHeight;
        }''', timeout=3000)
        # A filter reload follows the panel change, using Leaflet's actual
        # cached size and fitBounds. Every visible marker must still fit.
        page.evaluate('loadPhotos()')
        page.wait_for_function('''() => {
          const size = map.getSize();
          return markers.getLayers().length > 1 && markers.getLayers().every(marker => {
            const point = map.latLngToContainerPoint(marker.getLatLng());
            return point.x >= 0 && point.y >= 0 && point.x <= size.x && point.y <= size.y;
          });
        }''', timeout=3000)

    assert_current_size_and_fit()
    page.evaluate('toggleBottomPanel()')
    expect(page.locator('#bottomPanel')).to_have_class(re.compile(r'.*open.*'))
    assert_current_size_and_fit()
    handle = page.locator('#bpDragHandle').bounding_box()
    page.mouse.move(handle['x'] + 20, handle['y'] + 2)
    page.mouse.down()
    page.mouse.move(handle['x'] + 20, handle['y'] - 100, steps=5)
    page.mouse.up()
    assert_current_size_and_fit()
    page.evaluate('toggleBottomPanel()')
    assert_current_size_and_fit()
