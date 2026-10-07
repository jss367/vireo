// The Map page's status line: what the map is showing out of what, and a way
// to reach the photos it cannot plot. Classic script: load before map.html's
// inline script, which calls mapStatusHtml with each /api/photos/geo response.
'use strict';

function mapStatusHtml(data) {
  if (data.selection) return mapSelectionStatusHtml(data);
  var missingCoordinates = data.total_without_coordinates;
  if (data.photos.length === 0) {
    var emptyMessage = 'No geolocated photos found. Photos with EXIF GPS or an assigned map location will appear here.';
    if (missingCoordinates > 0) {
      emptyMessage += ' <a href="/browse?location_status=none">Browse ' +
        missingCoordinates + ' without coordinates</a>.';
    }
    return emptyMessage;
  }

  /* Map-coordinate coverage stats — global (unfiltered) for consistency */
  var totalPhotos = data.total_photos;
  var pct = totalPhotos > 0 ? Math.round((data.total_geolocated / totalPhotos) * 100) : 0;
  var parts;
  if (data.truncated) {
    parts = [
      'Showing ' + data.photos.length + ' of ' + data.total_filtered + ' matching photos',
      'Map display is limited to ' + data.render_limit + ' photos; refine the filters to see omitted results',
    ];
  } else {
    parts = ['Showing ' + data.total_filtered + ' of ' + data.total_geolocated + ' geolocated photos'];
  }
  parts.push(pct + '% map coverage');
  if (missingCoordinates > 0) {
    parts.push('<a href="/browse?location_status=none">' + missingCoordinates + ' without coordinates</a>');
  }
  return parts.join(' &mdash; ');
}

// A Browse selection opened with View on Map: the counts partition the
// selection, so every selected photo the map leaves off is accounted for.
function mapSelectionStatusHtml(data) {
  var sel = data.selection;
  var drawn = data.photos.length;
  var plural = function(n, one, many) {
    return n.toLocaleString() + ' ' + (n === 1 ? one : many);
  };
  var parts = [
    'Showing ' + drawn.toLocaleString() + ' of ' +
      plural(sel.selected, 'selected photo', 'selected photos'),
  ];
  if (sel.hidden_by_filters) {
    parts.push(sel.hidden_by_filters.toLocaleString() + ' hidden by map filters');
  }
  if (sel.without_location) {
    parts.push(sel.without_location.toLocaleString() + ' without a location');
  }
  if (sel.folder_unavailable) {
    parts.push(plural(sel.folder_unavailable,
      'in a folder that is offline or missing',
      'in folders that are offline or missing'));
  }
  if (sel.not_in_workspace) {
    parts.push(sel.not_in_workspace.toLocaleString() + ' no longer in this workspace');
  }
  if (sel.shown > drawn) {
    parts.push((sel.shown - drawn).toLocaleString() + ' beyond the map\u2019s ' +
      data.render_limit.toLocaleString() + '-photo display limit');
  }
  parts.push('<a href="/map">Show all photos</a>');
  return parts.join(' &mdash; ');
}
