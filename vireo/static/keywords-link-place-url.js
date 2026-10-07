/* ``/keywords?link_place=<keyword id>`` opens the Link-to-place dialog for
 * that keyword once the list has loaded. The Map page links here when "View
 * on Map" lands on a photo whose location is a name with no coordinates.
 *
 * Classic script loaded before keywords.html's inline script, which calls
 * openLinkPlaceFromUrl after the first loadKeywords(). The parameter is
 * dropped from the URL so a later reload does not reopen the dialog.
 */
function openLinkPlaceFromUrl(keywords, openLinkPlaceModal) {
  var url = new URL(window.location.href);
  var raw = url.searchParams.get('link_place');
  if (raw === null) return;
  url.searchParams.delete('link_place');
  window.history.replaceState(null, '', url.pathname + url.search + url.hash);
  var id = parseInt(raw, 10);
  var keyword = keywords.find(function(k) { return k.id === id; });
  if (keyword) {
    openLinkPlaceModal(keyword.id, keyword.name);
  } else if (typeof showToast === 'function') {
    showToast('That location keyword no longer exists.', 'error');
  }
}
