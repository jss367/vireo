// Multi-species helpers: the species field's list encoding, species identity
// keys, and a burst's confirmed species from its override or encounter.
// Classic page script; load boot.js after all definitions.

// -- Multi-species helpers --
//
// A burst can hold two confirmed species (two subjects in frame). The cache
// keeps the legacy single-valued fields (confirmed_species,
// species_override.species) as the primary and adds list-valued siblings
// (confirmed_species_list, species_override.species_list). The species field
// edits the whole set: comma-separated names, compared case-insensitively.

// Identity key for comparing species names. Mirrors the backend's
// keyword_match_key: whitespace-collapsed and ASCII-only case folded, the
// same equivalence SQLite's NOCASE gives add_keyword(). A Unicode-aware
// toLowerCase() would merge names the database keeps distinct (Éclair vs
// éclair) and make a replace/remove skip the tag it should drop.
/** @param {string|null|undefined} name */
function speciesKey(name) {
  return (name || '').trim().replace(/\s+/g, ' ').replace(/[A-Z]/g, function(ch) {
    return ch.toLowerCase();
  });
}

// Split the field into names. Names are separated by commas or semicolons;
// a name that itself contains a delimiter or a quote is written in double
// quotes, CSV style ("" inside quotes is a literal quote). joinSpecies()
// produces exactly this encoding, so a stored keyword such as
// `Mallard, domestic` round-trips through the field untouched.
/** @param {string} text */
function parseSpeciesList(text) {
  var out = /** @type {string[]} */ ([]);
  var seen = /** @type {Record<string,boolean>} */ ({});
  var parts = [];
  var buf = '';
  var quoted = false;
  var str = text || '';
  for (var i = 0; i < str.length; i++) {
    var ch = str[i];
    if (quoted) {
      if (ch === '"') {
        if (str[i + 1] === '"') { buf += '"'; i++; }
        else quoted = false;
      } else {
        buf += ch;
      }
    } else if (ch === '"') {
      quoted = true;
    } else if (ch === ',' || ch === ';') {
      parts.push(buf);
      buf = '';
    } else {
      buf += ch;
    }
  }
  parts.push(buf);
  parts.forEach(function(part) {
    var name = part.trim();
    if (!name) return;
    var key = speciesKey(name);
    if (seen[key]) return;
    seen[key] = true;
    out.push(name);
  });
  return out;
}

/** @param {string} name */
function quoteSpeciesName(name) {
  if (/[,;"]/.test(name)) return '"' + name.replace(/"/g, '""') + '"';
  return name;
}

/** @param {string[]|null|undefined} list */
function joinSpecies(list) {
  return (list || []).map(quoteSpeciesName).join(', ');
}

/** @param {string[]} a @param {string[]} b */
function sameSpeciesSet(a, b) {
  if (a.length !== b.length) return false;
  var keys = /** @type {Record<string,boolean>} */ ({});
  a.forEach(function(name) { keys[speciesKey(name)] = true; });
  return b.every(function(name) { return !!keys[speciesKey(name)]; });
}

/** @param {RapidOverride|null|undefined} ovr */
function overrideSpeciesList(ovr) {
  if (!ovr) return [];
  if (Array.isArray(ovr.species_list)) return ovr.species_list.filter(Boolean);
  return ovr.species ? [ovr.species] : [];
}

// An override speaks for the burst's confirmed set only when it is
// confirmed, or when it is the explicit-empty sentinel a remove leaves
// behind ({confirmed: false, species_list: []}). A detach-time candidate
// ({confirmed: false, species: guess}) is a display hint: it must never
// outrank the encounter's confirmed species, or a flag-only apply would
// "replace" the encounter species with the guess. Mirrors the backend's
// burst_species_list() and applyCurrent()'s alreadyConfirmedList.
/** @param {RapidOverride|null|undefined} ovr */
function overrideIsExplicit(ovr) {
  if (!ovr) return false;
  // A species_list array is authoritative whatever the confirmed flag:
  // confirmed (uniform frames), unconfirmed (a mixed burst after an edit —
  // its edited baseline, not the encounter's stale list, is what it holds)
  // or empty (the sentinel a remove leaves). Only the legacy shape without
  // the array is a detach-time candidate guess.
  if (Array.isArray(ovr.species_list)) return true;
  return ovr.confirmed === true && !!ovr.species;
}

/** @param {RapidEncounter|null|undefined} enc */
function encounterConfirmedList(enc) {
  if (!enc) return [];
  if (Array.isArray(enc.confirmed_species_list)) return enc.confirmed_species_list.filter(Boolean);
  return enc.confirmed_species ? [enc.confirmed_species] : [];
}

/** @param {RapidEncounter|null|undefined} enc @param {RapidBurst|null|undefined} burst */
function burstSpeciesList(enc, burst) {
  var ovr = burst && burst.species_override;
  if (overrideIsExplicit(ovr)) return overrideSpeciesList(ovr);
  var confirmed = encounterConfirmedList(enc);
  if (confirmed.length) return confirmed;
  // Nothing confirmed anywhere: an unconfirmed candidate override is still
  // the best prefill hint, ahead of the encounter's predicted label.
  var candidate = overrideSpeciesList(ovr);
  if (candidate.length) return candidate;
  if (enc && enc.species && enc.species.length && enc.species[0]) return [enc.species[0]];
  return [];
}

/** @param {RapidEncounter|null|undefined} enc @param {RapidBurst|null|undefined} burst */
function burstSpecies(enc, burst) {
  return joinSpecies(burstSpeciesList(enc, burst));
}

/** @param {RapidBurst|null|undefined} burst */
function hasBurstSpeciesOverride(burst) {
  return overrideIsExplicit(burst && burst.species_override);
}

// Find the burst that still holds every one of `ids` after the server
// returned a (possibly restructured) encounter list. A replace can
// auto-detach the burst into another encounter, shifting burst indices, so
// each follow-up species request must target the burst's new position.
/** @param {RapidEncounter[]} encounters @param {number[]} ids */
function locateBurstByPhotoIds(encounters, ids) {
  for (var e = 0; e < (encounters || []).length; e++) {
    var enc = /** @type {RapidEncounter} */ (encounters[e]);
    var bursts = enc.bursts && enc.bursts.length ? enc.bursts : [{ photo_ids: enc.photo_ids || [] }];
    for (var b = 0; b < bursts.length; b++) {
      var members = new Set(burstIds(bursts[b]));
      if (ids.length && ids.every(function(id) { return members.has(id); })) {
        return { encIdx: e, burstIdx: b };
      }
    }
  }
  return null;
}
