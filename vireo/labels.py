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
    """Prompt strings plus optional source identities; text remains unchanged."""

    def __init__(self, names=(), identities=None):
        super().__init__(names)
        self.identities = identities or {}


def _text_identity(names):
    return hashlib.sha256("\n".join(names).encode("utf-8")).hexdigest()


def _merge_identity(identities, name, entry):
    previous = identities.get(name)
    if previous is None:
        identities[name] = entry
    elif (previous.get("ambiguous") or entry.get("ambiguous")
          or previous.get("taxon_id") != entry.get("taxon_id")):
        identities[name] = {"ambiguous": True}
    else:
        # Names and ranks can change without changing the source taxon.
        # Keep metadata selection deterministic across label-set order.
        identities[name] = min((previous, entry), key=lambda value: json.dumps(value, sort_keys=True))

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
        list of species common names
    """
    filter_params = OBSERVATION_FILTERS.get(
        observation_filter, OBSERVATION_FILTERS["research"]
    )["params"]

    all_species = []
    identities = {}

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
                    if scientific_name and isinstance(taxon.get("id"), int):
                        _merge_identity(identities, name, {
                            "taxon_id": taxon["id"],
                            "scientific_name": scientific_name,
                            "common_name": common_name,
                            "rank": taxon.get("rank"),
                        })

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
        all_species.extend(group_species)

    if progress_callback:
        progress_callback(
            f"Done — {len(all_species)} total species",
            len(all_species),
            len(all_species),
        )

    return SpeciesLabels(all_species, identities)


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
            for name, entry in meta.get("label_identities", {}).items():
                if name not in names or not isinstance(entry, dict):
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
    # Import here rather than at module load: ``labels.py`` is imported
    # from environments (packaging, first-run bootstrap) that don't yet
    # have ``vireo/`` on ``sys.path``, and this helper is only reachable
    # once the app is running.
    from keyword_normalization import (
        keyword_match_key,
        normalize_keyword_display,
    )

    all_species = set()
    identities = {}
    consumed_metas = []
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
        consumed_metas.append(ls)
        for name, entry in labels.identities.items():
            _merge_identity(identities, keyword_match_key(name), entry)
        for name in labels:
            all_species.add(name)
    # Group by the ASCII-NOCASE key so case-only variants collapse the
    # same way SQLite's ``COLLATE NOCASE`` does, then collapse only the
    # groups that actually have more than one raw spelling.
    by_key = {}
    for name in all_species:
        by_key.setdefault(keyword_match_key(name) or name, []).append(name)
    merged = []
    for variants in by_key.values():
        if len(variants) == 1:
            # No collision: preserve the source spelling so the fingerprint
            # is byte-identical to what earlier runs hashed.
            merged.append(variants[0])
        else:
            # Genuine variant collision: prefer a spelling that
            # ``normalize_keyword_display`` leaves unchanged (i.e. one
            # already in the storage form, so the classifier's label and
            # what ``add_prediction`` writes agree byte-for-byte). Sort
            # first so both the primary preference and the fallback are
            # deterministic regardless of label-set order.
            ordered = sorted(variants)
            merged.append(next(
                (v for v in ordered if normalize_keyword_display(v) == v),
                ordered[0],
            ))
    merged_identities = {
        name: identities[keyword_match_key(name)] for name in merged
        if keyword_match_key(name) in identities
    }
    # Two regional lists can use different names for the same taxon. A
    # duplicate softmax class would split its probability before thresholding.
    # Only source-backed identities justify dropping a prompt; legacy text
    # keeps the historical spelling/fingerprint behavior above.
    seen_taxa = set()
    unique = []
    for name in sorted(merged):
        tid = merged_identities.get(name, {}).get("taxon_id")
        if tid is not None and tid in seen_taxa:
            continue
        if tid is not None:
            seen_taxa.add(tid)
        unique.append(name)
    return (
        SpeciesLabels(unique, {name: merged_identities[name] for name in unique if name in merged_identities}),
        consumed_metas,
    )
