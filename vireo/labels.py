"""Fetch regional species labels from iNaturalist for classification."""

import contextlib
import hashlib
import json
import logging
import os
import re
import ssl
import tempfile
import urllib.parse
import urllib.request

import certifi

log = logging.getLogger(__name__)

# Use certifi's CA bundle so HTTPS works on macOS without Install Certificates.command
_ssl_ctx = ssl.create_default_context(cafile=certifi.where())

INAT_API = "https://api.inaturalist.org/v1"

LABELS_DIR = os.path.expanduser("~/.vireo/labels")


class SpeciesLabels(list):
    """Prompt strings plus optional source identities; text remains unchanged.

    ``disambiguated`` and ``dropped_ambiguous`` record what
    :func:`disambiguate_labels` did to prompts two taxa would otherwise
    share, so callers can say so instead of presenting a silently shorter
    list (CORE_PHILOSOPHY: no black boxes). Both are plain lists of the
    final prompt strings and are not part of the label text itself.
    """

    def __init__(self, names=(), identities=None, disambiguated=(), dropped=()):
        super().__init__(names)
        self.identities = identities or {}
        self.disambiguated = list(disambiguated)
        self.dropped_ambiguous = list(dropped)


def _text_identity(names):
    return hashlib.sha256("\n".join(names).encode("utf-8")).hexdigest()


def _preferred_spelling(variants):
    """Pick one spelling for prompts that fold to the same keyword key.

    Prefer a spelling ``normalize_keyword_display`` leaves unchanged (i.e.
    already in the storage form, so the classifier's label and what
    ``add_prediction`` writes agree byte-for-byte). Sort first so both the
    preference and the fallback are deterministic regardless of label-set
    order.
    """
    from keyword_normalization import normalize_keyword_display

    if len(variants) == 1:
        return variants[0]
    ordered = sorted(variants)
    return next(
        (v for v in ordered if normalize_keyword_display(v) == v), ordered[0]
    )


def _representative_entry(members):
    """One identity for a taxon named by several sources.

    Names and ranks can change without changing the source taxon, so keep
    metadata selection deterministic across label-set order.
    """
    return min(
        (member[-1] for member in members),
        key=lambda value: json.dumps(value, sort_keys=True),
    )


def _taxon_key(entry):
    """What makes two identities the same taxon here.

    Identity first, binomial as the fallback: a source that names a
    species without an iNat ID still has to count as its own taxon.
    """
    entry = entry or {}
    return (entry.get("taxon_id")
            or (entry.get("scientific_name") or "").casefold()
            or None)


def _base_name(name, entry):
    """A prompt with its own scientific-name qualifier stripped.

    ``Honey Mushroom (Armillaria alpha)`` and a bare ``Honey Mushroom``
    for a second Armillaria are the same common name and must be weighed
    as one collision, or a merge of an already-qualified list with a list
    that never had to qualify would keep the bare prompt and hand the
    classifier back the ambiguity this module exists to remove. Both
    qualifiers this module writes count — the binomial and the
    ``(taxon N)`` fallback, which is equally persistable — and only a
    suffix that is the entry's *own* identity is stripped, so parentheses
    that belong to the name survive.
    """
    entry = entry or {}
    for qualifier in (entry.get("scientific_name"),
                      f"taxon {entry['taxon_id']}" if entry.get("taxon_id") else None):
        if not qualifier:
            continue
        suffix = f" ({qualifier})"
        if len(name) > len(suffix) and name.lower().endswith(suffix.lower()):
            return name[: -len(suffix)]
    return name


def _qualified_name(name, entry):
    """``Common Name (Scientific name)`` — the prompt for a shared name.

    Takes the whole identity, not just the binomial: the name may arrive
    carrying either qualifier this module writes, and re-qualifying
    ``Parrot (taxon 99999)`` without stripping its own suffix first would
    stack them.
    """
    from keyword_normalization import keyword_match_key

    scientific_name = (entry or {}).get("scientific_name")
    if not scientific_name:
        return name
    if keyword_match_key(name) == keyword_match_key(scientific_name):
        return name  # the prompt already *is* the binomial
    return f"{_base_name(name, entry)} ({scientific_name})"


def disambiguate_labels(records):
    """Split prompts that two different taxa would otherwise share.

    iNaturalist's ``preferred_common_name`` is not unique: a single
    regional list gives "Honey Mushroom" to several *Armillaria* species,
    and case-only variants (``Chia`` = *Salvia columbariae* vs ``chia`` =
    *Salvia hispanica*) collide again once folded through
    ``keyword_match_key``. Feeding the classifier one prompt for two taxa
    means every prediction on it is unattributable, so ``Classifier``
    refuses the whole label set.

    ``records`` is a list of ``(name, identity_or_None)`` in source order.
    Groups that name a single taxon (or carry no source identity at all)
    are collapsed exactly as before, so their prompt text — and therefore
    ``compute_fingerprint`` and every cached run keyed on it — is
    unchanged. A group naming two or more taxa is rewritten to ``Common
    Name (Scientific name)`` per taxon, which ``SpeciesResolver`` already
    reads back as explicit taxon evidence.

    Returns ``(names, identities, disambiguated, dropped, sources)``.
    ``dropped`` holds prompts no source can pin to one taxon — a label
    file written before this rewrite existed stores only
    ``{"ambiguous": True}`` for those, discarding the scientific names
    needed to split them, so they are left out rather than poisoning the
    run. Re-downloading the list recovers them. ``sources`` maps each
    surviving prompt to the indices of the records that produced it,
    which is the only exact answer to "did this file back a class" —
    every proxy for it (the name, the taxon, the collision group) is
    shared with records that did not.
    """
    # Import here rather than at module load: ``labels.py`` is imported
    # from environments (packaging, first-run bootstrap) that don't yet
    # have ``vireo/`` on ``sys.path``, and this helper is only reachable
    # once the app is running. Same for the other keyword_normalization
    # imports in this module.
    from keyword_normalization import keyword_match_key

    groups = {}
    for index, (name, entry) in enumerate(records):
        # Key on the unqualified name so an already-split prompt and a
        # bare one for a sibling taxon meet in the same group.
        key = keyword_match_key(_base_name(name, entry)) or name
        groups.setdefault(key, []).append((index, name, entry or {}))

    emitted, dropped = [], []
    for group in groups.values():
        by_taxon = {}
        unattributed = []
        for index, name, entry in group:
            scientific_name = entry.get("scientific_name")
            if entry.get("ambiguous") or not scientific_name:
                unattributed.append((index, name, entry))
            else:
                by_taxon.setdefault(_taxon_key(entry), []).append(
                    (index, name, entry)
                )
        contested = len(by_taxon) > 1 or any(
            entry.get("ambiguous") for _i, _name, entry in unattributed
        )
        if not contested:
            # One taxon, or none at all: keep the historical spelling fold.
            # Every record in the group backs the surviving prompt, including
            # a spelling that lost the fold — it named the same species.
            spelling = _preferred_spelling([name for _i, name, _e in group])
            members = next(iter(by_taxon.values()), [])
            entry = _representative_entry(members) if members else None
            emitted.append([spelling, entry, spelling, False,
                            {index for index, _n, _e in group}])
            continue
        for members in by_taxon.values():
            spelling = _preferred_spelling([name for _i, name, _e in members])
            entry = _representative_entry(members)
            qualified = _qualified_name(spelling, entry)
            emitted.append([qualified, entry, spelling, True,
                            {index for index, _n, _e in members}])
        # A prompt with no scientific name in a contested group cannot be
        # qualified — and must not stand, because it would answer for
        # whichever taxon the model happened to mean.
        dropped.extend(name for _i, name, _e in unattributed)

    # Two groups can still land on one string: a source whose common name
    # literally reads "Foo (Alpha beta)" for another taxon, or two taxon
    # IDs sharing a binomial. Taking the collision after every group has
    # spoken keeps the outcome independent of label-set order, and the
    # taxon form is one ``SpeciesResolver.explicit_source`` already reads.
    # Fold the claim key the way SQLite and ``add_prediction`` do: two
    # prompts that differ only in case are one keyword downstream, so a
    # shared binomial under two taxon IDs collides even when the strings
    # are not byte-identical.
    def contested_names(records):
        claims = {}
        for record in records:
            claims.setdefault(keyword_match_key(record[0]), set()).add(
                _taxon_key(record[1])
            )
        return {key for key, taxa in claims.items() if len(taxa) > 1}

    settled = []
    disputed = contested_names(emitted)
    for name, entry, spelling, was_split, sources in emitted:
        if keyword_match_key(name) in disputed:
            taxon_id = (entry or {}).get("taxon_id")
            if not taxon_id:
                # Nothing left to tell it apart by; it would answer for a
                # taxon that is not its own.
                dropped.append(name)
                continue
            # Strip whichever qualifier the spelling already carries, so a
            # reloaded fallback is re-qualified rather than stacked.
            name = f"{_base_name(spelling, entry)} (taxon {taxon_id})"
            was_split = True
        settled.append([name, entry, spelling, was_split, sources])

    # A generated fallback can land on a name a third source already uses
    # verbatim. Nothing is left to qualify by at that point, so keep the
    # claimant the prompt actually names — ``explicit_source`` reads the
    # taxon out of the string, and attributing it to anyone else would be
    # wrong — and report the rest rather than overwriting an identity.
    still_disputed = contested_names(settled)
    winner = {}
    for index in sorted(
        (i for i, r in enumerate(settled)
         if keyword_match_key(r[0]) in still_disputed),
        key=lambda i: (
            # The prompt names a taxon: that claimant, not another, is who
            # ``explicit_source`` would read out of it.
            f" (taxon {(settled[i][1] or {}).get('taxon_id')})" not in settled[i][0],
            json.dumps(settled[i][1], sort_keys=True),
        ),
    ):
        winner.setdefault(keyword_match_key(settled[index][0]), index)

    names, identities, disambiguated, sources = [], {}, [], {}
    for index, (name, entry, _spelling, was_split, record_ids) in enumerate(settled):
        key = keyword_match_key(name)
        if key in still_disputed and winner[key] != index:
            dropped.append(name)
            continue
        names.append(name)
        sources[name] = record_ids
        if entry:
            identities[name] = entry
        if was_split:
            disambiguated.append(name)
    return names, identities, disambiguated, dropped, sources


# Major taxonomic groups with their iNaturalist taxon IDs
TAXON_GROUPS = {
    "birds": {"id": 3, "name": "Birds", "class": "Aves"},
    "mammals": {"id": 40151, "name": "Mammals", "class": "Mammalia"},
    "reptiles": {"id": 26036, "name": "Reptiles", "class": "Reptilia"},
    "amphibians": {"id": 20978, "name": "Amphibians", "class": "Amphibia"},
    "insects": {"id": 47158, "name": "Insects", "class": "Insecta"},
    "plants": {"id": 47126, "name": "Plants", "kingdom": "Plantae"},
    "fungi": {"id": 47170, "name": "Fungi", "kingdom": "Fungi"},
}


def search_places(query):
    """Search for places by name using iNaturalist API.

    Args:
        query: search string (e.g., "California", "Virginia")

    Returns:
        list of {id, name, display_name, place_type}
    """
    params = urllib.parse.urlencode({"q": query})
    url = f"{INAT_API}/places/autocomplete?{params}"
    log.info("Searching iNaturalist places: %s", query)

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Vireo/1.0"})
        with urllib.request.urlopen(req, timeout=10, context=_ssl_ctx) as resp:
            data = json.loads(resp.read())
    except Exception:
        log.warning(
            "Failed to search iNaturalist places for '%s'", query, exc_info=True
        )
        return []

    results = []
    for r in data.get("results", []):
        results.append(
            {
                "id": r["id"],
                "name": r.get("name", ""),
                "display_name": r.get("display_name", r.get("name", "")),
            }
        )
    log.info("Found %d places for '%s'", len(results), query)
    return results


OBSERVATION_FILTERS = {
    "research": {
        "name": "Research grade",
        "description": "Community-verified wild observations only",
        "params": {"quality_grade": "research"},
    },
    "wild": {
        "name": "Wild only",
        "description": "All wild observations, including unverified",
        "params": {"captive": "false"},
    },
    "all": {
        "name": "All observations",
        "description": "Includes zoo and captive sightings",
        "params": {},
    },
}


def fetch_species_list(
    place_id, taxon_groups, observation_filter="research", progress_callback=None
):
    """Fetch species observed in a region from iNaturalist.

    Args:
        place_id: iNaturalist place ID (e.g., 14 for California)
        taxon_groups: list of group keys from TAXON_GROUPS (e.g., ['birds', 'mammals'])
        observation_filter: one of 'research', 'wild', 'all'
        progress_callback: optional callable(message, current=None, total=None)

    Returns:
        SpeciesLabels of species prompts. Names two taxa share are
        rewritten to ``Common Name (Scientific name)`` by
        ``disambiguate_labels`` before the list is returned, so a fetch
        that spans plants and fungi cannot hand the classifier one prompt
        for several species.
    """
    filter_params = OBSERVATION_FILTERS.get(
        observation_filter, OBSERVATION_FILTERS["research"]
    )["params"]

    records = []

    for gi, group_key in enumerate(taxon_groups):
        group = TAXON_GROUPS.get(group_key)
        if not group:
            continue

        taxon_id = group["id"]
        group_name = group["name"]
        group_prefix = f"[{gi + 1}/{len(taxon_groups)}] {group_name}"

        if progress_callback:
            progress_callback(f"{group_prefix}: starting...", 0, 0)

        page = 1
        per_page = 500
        group_species = []
        group_total = 0

        while True:
            params = urllib.parse.urlencode(
                {
                    "place_id": place_id,
                    "taxon_id": taxon_id,
                    "per_page": per_page,
                    "page": page,
                    **filter_params,
                }
            )
            url = f"{INAT_API}/observations/species_counts?{params}"

            data = None
            for attempt in range(3):
                try:
                    req = urllib.request.Request(
                        url, headers={"User-Agent": "Vireo/1.0"}
                    )
                    with urllib.request.urlopen(req, timeout=60, context=_ssl_ctx) as resp:
                        data = json.loads(resp.read())
                    break
                except Exception:
                    log.warning(
                        "Fetch attempt %d failed for page %d of %s",
                        attempt + 1,
                        page,
                        group_name,
                        exc_info=True,
                    )
                    if attempt < 2:
                        import time

                        time.sleep(2)
                    else:
                        if progress_callback:
                            progress_callback(
                                f"{group_prefix}: failed after 3 attempts on page {page}",
                                0,
                                0,
                            )

            if data is None:
                break

            results = data.get("results", [])
            if not results:
                break

            group_total = data.get("total_results", 0)

            for r in results:
                taxon = r.get("taxon", {})
                common_name = taxon.get("preferred_common_name", "")
                scientific_name = taxon.get("name", "")
                name = common_name or scientific_name
                if name:
                    group_species.append(name)
                    entry = None
                    if scientific_name and isinstance(taxon.get("id"), int):
                        entry = {
                            "taxon_id": taxon["id"],
                            "scientific_name": scientific_name,
                            "common_name": common_name,
                            "rank": taxon.get("rank"),
                        }
                    records.append((name, entry))

            fetched = (page - 1) * per_page + len(results)

            if progress_callback:
                progress_callback(
                    f"{group_prefix}: {fetched}/{group_total} species",
                    fetched,
                    group_total,
                )

            if fetched >= group_total:
                break
            page += 1

        log.info(
            "Fetched %d %s species for place %d",
            len(group_species),
            group_name,
            place_id,
        )

    names, identities, disambiguated, dropped, _sources = disambiguate_labels(
        records
    )
    if disambiguated:
        log.info(
            "%d fetched prompts shared a common name with another taxon and "
            "were qualified with its scientific name", len(disambiguated),
        )

    if progress_callback:
        progress_callback(
            f"Done — {len(names)} total species",
            len(names),
            len(names),
        )

    return SpeciesLabels(names, identities, disambiguated, dropped)


def save_labels(name, place_id, place_name, taxon_groups, species,
                 observation_filter="research"):
    """Save a labels list to disk.

    Args:
        name: label set name (e.g., "California Birds")
        place_id: iNaturalist place ID
        place_name: human-readable place name
        taxon_groups: list of group keys used
        species: list of species names
        observation_filter: one of 'research', 'wild', 'all'

    Returns:
        path to saved labels file
    """
    os.makedirs(LABELS_DIR, exist_ok=True)

    slug = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
    labels_path = os.path.join(LABELS_DIR, f"{slug}.txt")
    meta_path = os.path.join(LABELS_DIR, f"{slug}.json")

    # Write labels file (one per line)
    species_text = "".join(sp + "\n" for sp in sorted(set(species)))
    _atomic_write_text(labels_path, species_text)

    # Write metadata
    filter_info = OBSERVATION_FILTERS.get(
        observation_filter, OBSERVATION_FILTERS["research"]
    )
    meta = {
        "name": name,
        "place_id": place_id,
        "place_name": place_name,
        "taxon_groups": taxon_groups,
        "observation_filter": observation_filter,
        "observation_filter_name": filter_info["name"],
        "species_count": len(set(species)),
        "labels_file": labels_path,
    }
    identities = getattr(species, "identities", {})
    if identities:
        meta["label_identities"] = identities
        meta["labels_text_sha256"] = _text_identity(sorted(set(species)))
    _atomic_write_text(meta_path, json.dumps(meta, indent=2))

    return labels_path


def read_label_file(path):
    """Read a label file, returning its stripped, non-empty lines.

    UTF-8 with a cp1252 fallback: label files hold non-ASCII species names
    and current writes are explicitly UTF-8, but a Windows install that
    saved a label set before the writer became explicit stored the file in
    the locale default (cp1252). Reading that legacy file strictly as UTF-8
    raises ``UnicodeDecodeError`` on the first non-ASCII byte and silently
    drops the whole active set. Any caller opening a label file directly
    must route through here so the fallback protects every code path, not
    just ``load_merged_labels``.
    """
    try:
        with open(path, encoding="utf-8") as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        log.warning(
            "Label file %s is not valid UTF-8; falling back to cp1252 "
            "for legacy Windows-written files",
            path,
        )
        with open(path, encoding="cp1252") as f:
            lines = f.readlines()
    names = [line.strip() for line in lines if line.strip()]
    identities = {}
    try:
        with open(os.path.splitext(path)[0] + ".json", encoding="utf-8") as f:
            meta = json.load(f)
        # Do not attach an old source identity after a user edits the prompts,
        # or during the small window between the two atomic file replacements.
        if meta.get("labels_text_sha256") == _text_identity(names):
            # Set membership, not ``in names``: a 26k-species regional list
            # otherwise costs 26k linear scans (~2s per read, on every
            # classify job and readiness check).
            present = set(names)
            for name, entry in meta.get("label_identities", {}).items():
                if name not in present or not isinstance(entry, dict):
                    continue
                if entry.get("ambiguous"):
                    identities[name] = {"ambiguous": True}
                elif (type(entry.get("taxon_id")) is int and entry["taxon_id"] > 0
                      and isinstance(entry.get("scientific_name"), str)
                      and entry["scientific_name"].strip()):
                    identities[name] = entry
    except (OSError, ValueError, TypeError, AttributeError):
        pass  # Legacy and hand-authored text files remain supported.
    return SpeciesLabels(names, identities)


def _atomic_write_text(path, text):
    """Write text to ``path`` via a sibling temp file + os.replace().

    Two concurrent label-fetch jobs targeting the same slug can otherwise
    interleave bytes inside a half-written .txt or .json. Atomic rename
    guarantees that any reader sees either the old contents or the full new
    contents, never a partial mix.
    """
    directory = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(
        prefix=f".{os.path.basename(path)}.", suffix=".tmp", dir=directory
    )
    try:
        # Explicit UTF-8: label files hold species names with non-ASCII
        # characters (`Köhler’s Vine Snake`, `Hawaiʻi ʻamakihi`), and Python's
        # default text encoding is the locale codepage on Windows (cp1252),
        # which cannot represent U+02BB at all.
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp_path, path)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise


def delete_labels(labels_file):
    """Delete a label set from disk and deactivate if active.

    Args:
        labels_file: path to the .txt labels file
    """
    # Remove .txt and corresponding .json
    if os.path.exists(labels_file):
        os.remove(labels_file)
    meta_path = labels_file.rsplit(".", 1)[0] + ".json"
    if os.path.exists(meta_path):
        os.remove(meta_path)

    # Remove from active labels if present
    active = get_active_labels()
    active_paths = [a["labels_file"] for a in active if a.get("labels_file") != labels_file]
    set_active_labels(active_paths)

    log.info("Deleted label set: %s", labels_file)


def get_saved_labels():
    """List all saved label sets.

    Returns:
        list of {name, place_name, taxon_groups, species_count, labels_file}
    """
    if not os.path.isdir(LABELS_DIR):
        return []

    result = []
    for fname in sorted(os.listdir(LABELS_DIR)):
        if fname.endswith(".json"):
            try:
                with open(os.path.join(LABELS_DIR, fname)) as f:
                    meta = json.load(f)
                result.append(meta)
            except Exception:
                pass
    return result


def get_active_labels():
    """Return the list of currently active label set metadata objects.

    Returns:
        list of metadata dicts (each has at least 'labels_file').
        Empty list if nothing is configured or files are missing.
    """
    config_path = os.path.expanduser("~/.vireo/labels_active.json")
    if not os.path.exists(config_path):
        return []

    try:
        with open(config_path) as f:
            data = json.load(f)
    except Exception:
        return []

    # New format: {"active_labels": ["/path/a.txt", "/path/b.txt"]}
    if "active_labels" in data and isinstance(data["active_labels"], list):
        paths = data["active_labels"]
    elif "labels_file" in data:
        # Old format: single object — migrate to list
        paths = [data["labels_file"]]
    else:
        return []

    # Resolve metadata for each path, skip missing files
    saved = get_saved_labels()
    saved_by_file = {s["labels_file"]: s for s in saved}
    result = []
    for p in paths:
        if not p or not os.path.exists(p):
            log.warning("Active label file missing, skipping: %s", p)
            continue
        meta = saved_by_file.get(p)
        if meta:
            result.append(meta)
        else:
            result.append({"labels_file": p})
    return result


def set_active_labels(labels_files):
    """Set the active label files.

    Args:
        labels_files: list of label file paths, or a single path string
                      (for backward compat).
    """
    config_path = os.path.expanduser("~/.vireo/labels_active.json")
    if isinstance(labels_files, str):
        labels_files = [labels_files]
    with open(config_path, "w") as f:
        json.dump({"active_labels": labels_files}, f, indent=2)


def load_merged_labels(label_sets):
    """Read and merge species from multiple label sets.

    Thin wrapper around ``load_merged_labels_with_metas`` that discards the
    consumed-metas list. Prefer the ``_with_metas`` variant when the caller
    needs to know which sets actually contributed to the returned labels —
    e.g. to name label sources without claiming a file that was deleted
    between the caller's existence check and this loader's read.

    Args:
        label_sets: list of metadata dicts, each with a 'labels_file' key.

    Returns:
        sorted, deduplicated list of species name strings.

    Dedupes by the same ASCII-NOCASE key that ``add_prediction``,
    alternative dedupe, and burst consensus use downstream — matching
    SQLite's ``COLLATE NOCASE`` semantics via ``keyword_match_key``.
    Some bundled label files carry curly apostrophes (`Bosc's
    Fringe-toed lizard`, `Geoffroy's Tamarin`) while others use the
    plain ASCII form, and hand-edited sets can differ only in case
    (`Say's Phoebe` vs `Say's phoebe`). Handing the classifier two
    spellings of the same species feeds its softmax two near-duplicate
    classes that split probability between them — because each result
    is thresholded independently in ``_build_custom_results``, a valid
    prediction can fall below the configured threshold or lose an
    alternative slot. Grouping by ``normalize_keyword_display`` alone
    would miss the case-only collision because that helper preserves
    case; ``keyword_match_key`` composes it with the same ASCII-only
    lowercase table SQLite uses.

    The fold decides only which of two COLLIDING spellings to drop; a
    label whose folded key is unique keeps its original source spelling.
    That distinction matters because ``compute_fingerprint(labels)``
    hashes this exact list and ``classifier_runs`` is keyed on the
    result: rewriting a non-colliding label (e.g. the lone `Bosc's
    Fringe-toed lizard` in california-us-reptiles, or `'Anianiau` in the
    Hawaii set) would change the fingerprint of five of the six shipped
    label sets and strand ~70k cached runs, re-running inference over
    the whole catalog for no dedupe benefit.
    """
    labels, _ = load_merged_labels_with_metas(label_sets)
    return labels


def load_label_set(path, meta=None):
    """One file's labels exactly as the classify job will see them.

    ``read_label_file`` returns the raw prompt text. Every run the UI can
    start sends ``labels_files``, which routes through
    ``load_merged_labels``: it folds spelling variants, drops duplicate
    taxa and splits names two species share into ``Common Name
    (Scientific name)``. Callers that only *report* on a set — the
    embedding matrix, the cached-embedding check, the classify readiness
    panel — must ask the same question the job answers, or they describe
    and precompute a list that is never classified.

    That holds for a file with no identity sidecar too: ``Robin`` and
    ``robin`` are two prompts on disk and one class at runtime, so
    reporting the raw pair would name a count and warm an embedding
    identity the run never uses.
    """
    return load_merged_labels([meta or {"labels_file": path}])


def load_merged_labels_with_metas(label_sets):
    """Read and merge species, returning the metadata of the sets consumed.

    Same as ``load_merged_labels`` (see its docstring for dedupe/fingerprint
    semantics), plus a second return value: the sublist of ``label_sets``
    whose file was actually opened and read in this same pass. Sets whose
    file is missing at read time — the ``load_merged_labels`` code path
    already skips them silently — are dropped from the returned metadata
    too, so callers naming the label source cannot claim a list that
    contributed nothing. Doing this in the same pass closes the race
    between a caller's existence check and this loader's read: the Settings
    DELETE endpoint can retire a label file while the classify job is
    loading, so any two-pass check disagrees with reality.
    """
    index_of = {}
    records = []
    read_sets = []
    for ls in label_sets:
        path = ls.get("labels_file", "")
        if not path or not os.path.exists(path):
            log.warning("Label file missing, skipping: %s", path)
            continue
        try:
            labels = read_label_file(path)
        except FileNotFoundError:
            # Racing DELETE between os.path.exists above and the open() in
            # read_label_file — treat identically to the exists() miss so
            # consumed_metas reflects only files we actually read.
            log.warning("Label file vanished during read, skipping: %s", path)
            continue
        own = set()
        for name in labels:
            entry = labels.identities.get(name)
            key = (name, json.dumps(entry, sort_keys=True))
            if key not in index_of:
                # The same prompt in two sets is one record, referenced by
                # both — so a duplicate still credits the file that holds it.
                index_of[key] = len(records)
                records.append((name, entry))
            own.add(index_of[key])
        read_sets.append((ls, own))
    # ``disambiguate_labels`` groups by the ASCII-NOCASE key so case-only
    # variants collapse the same way SQLite's ``COLLATE NOCASE`` does,
    # keeps the source spelling of every group that names one taxon, and
    # splits the groups where two sources — or two entries of one source —
    # mean different species by the same name.
    merged, merged_identities, disambiguated, dropped, record_sources = (
        disambiguate_labels(records)
    )
    if dropped:
        log.warning(
            "Skipping %d label(s) whose name refers to multiple taxa with no "
            "scientific name on record; re-download the list to restore them: %s",
            len(dropped), ", ".join(sorted(set(dropped))[:20]),
        )
    # Two regional lists can use different names for the same taxon. A
    # duplicate softmax class would split its probability before thresholding.
    # Only source-backed identities justify dropping a prompt; legacy text
    # keeps the historical spelling/fingerprint behavior above.
    seen_taxa = set()
    seen_names = set()
    unique = []
    for name in sorted(merged):
        tid = merged_identities.get(name, {}).get("taxon_id")
        if tid is not None and tid in seen_taxa:
            continue
        if name in seen_names:
            continue  # a qualified name can meet a source that already used it
        if tid is not None:
            seen_taxa.add(tid)
        seen_names.add(name)
        unique.append(name)
    kept = set(unique)
    kept_identities = {
        name: merged_identities[name] for name in kept if name in merged_identities
    }
    # A file backs a class only if one of ITS records produced a prompt
    # that survived. Anything less exact credits a file whose prompts were
    # all dropped — and then ``describe_label_source`` names it,
    # ``labels_fingerprints`` records it, and deleting that useless file
    # later makes an otherwise unchanged merged run look stale.
    backing = set()
    for name in unique:
        backing |= record_sources.get(name, set())
    consumed_metas = []
    for ls, own in read_sets:
        if own & backing:
            consumed_metas.append(ls)
        else:
            log.warning(
                "Label file %s contributed no usable species to this merge; "
                "not recording it as a label source",
                ls.get("labels_file", ""),
            )
    return (
        SpeciesLabels(
            unique,
            kept_identities,
            [name for name in sorted(set(disambiguated)) if name in kept],
            sorted(set(dropped)),
        ),
        consumed_metas,
    )
