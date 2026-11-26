// Keeping Representative badges and the applied flag filter current after a
// Life List change or a flag change made in the shared lightbox.
// ES module.

import { state, CATEGORY_ORDER } from './state.js';
import { render } from './render.js';
import { _clearMissRepresentativeStateIfIneligible, refreshAppliedFlagFilter } from './flagging.js';

// setLifeListPhoto (miss card context menu / shared lightbox action) only emits
// a lifelist:changed event and updates its own caches; without this listener
// the Misses cards keep the pre-change value of p.is_species_representative, so
// the newly assigned photo stays un-badged and any previously visible former
// representative keeps its badge until a full reload. Walk missesData across
// all categories in place, flip the species entry's flags, and rerender.
export function bindMissLifeListEvents() {
  document.addEventListener('lifelist:changed', function(e) {
    var detail = e && e.detail ? e.detail : {};
    const species = detail.species;
    const photoId = detail.photoId;
    if (!species || !photoId) return;
    var changed = false;
    CATEGORY_ORDER.forEach(function(cat) {
      var photos = state.missesData[cat] || [];
      photos.forEach(function(p) {
        var entries = Array.isArray(p.life_list) ? p.life_list : [];
        var touched = false;
        entries.forEach(function(entry) {
          if (!entry || entry.species !== species) return;
          var isCurrent = p.id === photoId;
          if (entry.is_current_photo !== isCurrent || entry.is_species_representative !== isCurrent) {
            entry.is_current_photo = isCurrent;
            entry.is_species_representative = isCurrent;
            touched = true;
          }
        });
        if (p.id === photoId && !entries.some(function(entry) { return entry && entry.species === species; })) {
          entries.push({species: species, is_current_photo: true, is_species_representative: true});
          p.life_list = entries;
          touched = true;
        }
        var isRep = entries.some(function(entry) {
          return entry && entry.is_species_representative;
        });
        if (p.is_species_representative !== isRep) {
          p.is_species_representative = isRep;
          touched = true;
        }
        if (touched) changed = true;
      });
    });
    if (changed) render();
  });
}

// Synchronize the grid after a photo flag changes in the lightbox.
export function bindMissLightboxFlagEvents() {
  document.addEventListener('lightbox:flagchanged', function(e) {
    var detail = e && e.detail ? e.detail : {};
    const photoId = detail.photoId;
    var flag = detail.flag;
    if (photoId == null) return;
    if (_clearMissRepresentativeStateIfIneligible([photoId], flag)) {
      render();
    }
    refreshAppliedFlagFilter();
  });
}
