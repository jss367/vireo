/* "View on Map" for a photo the map cannot place.
 *
 * When the requested photo is not among the plottable photos,
 * ``/api/photos/geo?photo_id=`` returns no markers and an
 * ``unplottable_focus`` object saying why. The Map page shows this notice
 * instead of the rest of the library, so a map full of other photos is never
 * presented as the answer to "where was this photo taken".
 *
 * Classic script loaded before map.html's inline script. The pure
 * describeUnplottableFocus is unit-tested in vireo/tests/map_focus.cjs.
 */

function describeUnplottableFocus(focus) {
  var id = focus && focus.id;
  var name = (focus && focus.filename) || 'This photo';
  var browse = { label: 'Open in Browse', href: '/browse?photo_id=' + encodeURIComponent(id) };
  if (!focus || focus.reason === 'not_found') {
    return {
      title: 'Photo not found',
      detail: 'This photo is not in the active workspace, so the map has nothing to show for it.',
      actions: [],
    };
  }
  if (focus.reason === 'unavailable') {
    return {
      title: name + ' is not on the map',
      detail: 'Its folder is offline or missing, '
        + 'so the map cannot show it right now.',
      actions: [browse],
    };
  }
  var locations = focus.location_keywords || [];
  if (!locations.length) {
    return {
      title: name + ' has no map location',
      detail: 'It has no EXIF GPS coordinates and no assigned location.',
      actions: [{ label: 'Assign a location in Browse', href: browse.href }],
    };
  }
  var quoted = locations.map(function(k) { return '“' + k.name + '”'; });
  var detail = locations.length === 1
    ? 'It has no EXIF GPS coordinates, and its location ' + quoted[0]
      + ' is a name only, not linked to a place with map coordinates. '
      + 'Linking it puts this photo, and every other photo with that location, on the map.'
    : 'It has no EXIF GPS coordinates, and its locations ' + quoted.join(', ')
      + ' are names only, not linked to places with map coordinates. '
      + 'Linking one puts this photo, and every other photo with that location, on the map.';
  return {
    title: name + ' has no map location',
    detail: detail,
    actions: locations.map(function(k) {
      return {
        label: 'Link “' + k.name + '” to a place',
        href: '/keywords?link_place=' + encodeURIComponent(k.id),
      };
    }).concat([browse]),
  };
}

function hideUnplottableFocusNotice() {
  var notice = document.getElementById('mapFocusNotice');
  if (notice) notice.hidden = true;
}

/* Show the notice for ``focus``. ``onShowAll`` reloads the map without the
 * deep link; the URL drops ``photo_id`` too, so a reload stays on the full
 * map instead of bringing the notice back. */
function showUnplottableFocusNotice(focus, onShowAll) {
  var view = describeUnplottableFocus(focus);
  var notice = document.getElementById('mapFocusNotice');
  notice.textContent = '';

  var title = document.createElement('div');
  title.className = 'map-focus-notice-title';
  title.textContent = view.title;
  var detail = document.createElement('p');
  detail.textContent = view.detail;
  var actions = document.createElement('div');
  actions.className = 'map-focus-notice-actions';
  view.actions.forEach(function(action) {
    var link = document.createElement('a');
    link.className = 'btn btn-primary';
    link.href = action.href;
    link.textContent = action.label;
    actions.appendChild(link);
  });
  var showAll = document.createElement('button');
  showAll.type = 'button';
  showAll.className = 'btn btn-secondary';
  showAll.textContent = 'Show all photos on the map';
  showAll.addEventListener('click', function() {
    var url = new URL(window.location.href);
    url.searchParams.delete('photo_id');
    window.history.replaceState(null, '', url.pathname + url.search + url.hash);
    hideUnplottableFocusNotice();
    onShowAll();
  });
  actions.appendChild(showAll);

  notice.appendChild(title);
  notice.appendChild(detail);
  notice.appendChild(actions);
  notice.hidden = false;
  document.getElementById('mapStatus').textContent = view.title + '.';
}
