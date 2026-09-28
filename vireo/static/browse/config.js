/* Browse: /api/config fetch, keyboard shortcut defaults, config application.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

var _shortcuts = null;
var _BROWSE_SC_DEFAULTS = {rate_0:'0',rate_1:'1',rate_2:'2',rate_3:'3',rate_4:'4',rate_5:'5',
  flag:'p',reject:'x',unflag:'u',undo:'ctrl+z',redo:'ctrl+shift+z',select_all:'ctrl+a',zoom:'z',
  compare:'c',color_red:'6',color_yellow:'7',color_green:'8',color_blue:'9'};
var _cfgPromise = (async function() {
  try {
    var cfg = await safeFetch('/api/config', {}, { toast: false });
    var saved = (cfg.keyboard_shortcuts || {}).browse || {};
    _shortcuts = Object.assign({}, _BROWSE_SC_DEFAULTS, saved);
    window._vireoShortcuts = cfg.keyboard_shortcuts || {};
    window.GOOGLE_MAPS_API_KEY = cfg.google_maps_api_key || '';
    window.GOOGLE_MAPS_PREFER_ENGLISH = cfg.google_maps_prefer_english !== false;
    return cfg;
  } catch(e) {
    _shortcuts = Object.assign({}, _BROWSE_SC_DEFAULTS);
    return null;
  }
})();

function applyBrowseConfig(cfg) {
  if (!cfg) return;
  if (cfg.photos_per_page) perPage = cfg.photos_per_page;
  if (cfg.browse_thumb_default && VireoViewPreferences.read('vireo.browse.thumbSize') === null) {
    var slider = document.getElementById('thumbSizeSlider');
    if (slider) {
      slider.value = cfg.browse_thumb_default;
      slider.dispatchEvent(new Event('input'));
    }
  }
  if (Array.isArray(cfg.browse_card_fields)) cardFields = cfg.browse_card_fields;
}
