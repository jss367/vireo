"""Fetch regional species labels from iNaturalist for classification."""

import json
import logging
import os
import urllib.request
import urllib.parse

log = logging.getLogger(__name__)

INAT_API = "https://api.inaturalist.org/v1"

LABELS_DIR = os.path.expanduser("~/.spotter/labels")

# Major taxonomic groups with their iNaturalist taxon IDs
TAXON_GROUPS = {
    'birds': {'id': 3, 'name': 'Birds', 'class': 'Aves'},
    'mammals': {'id': 40151, 'name': 'Mammals', 'class': 'Mammalia'},
    'reptiles': {'id': 26036, 'name': 'Reptiles', 'class': 'Reptilia'},
    'amphibians': {'id': 20978, 'name': 'Amphibians', 'class': 'Amphibia'},
    'insects': {'id': 47158, 'name': 'Insects', 'class': 'Insecta'},
    'plants': {'id': 47126, 'name': 'Plants', 'kingdom': 'Plantae'},
    'fungi': {'id': 47170, 'name': 'Fungi', 'kingdom': 'Fungi'},
}


def search_places(query):
    """Search for places by name using iNaturalist API.

    Args:
        query: search string (e.g., "California", "Virginia")

    Returns:
        list of {id, name, display_name, place_type}
    """
    params = urllib.parse.urlencode({'q': query})
    url = f"{INAT_API}/places/autocomplete?{params}"
    log.info("Searching iNaturalist places: %s", query)

    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Spotter/1.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception:
        log.warning("Failed to search iNaturalist places for '%s'", query, exc_info=True)
        return []

    results = []
    for r in data.get('results', []):
        results.append({
            'id': r['id'],
            'name': r.get('name', ''),
            'display_name': r.get('display_name', r.get('name', '')),
        })
    log.info("Found %d places for '%s'", len(results), query)
    return results


def fetch_species_list(place_id, taxon_groups, progress_callback=None):
    """Fetch species observed in a region from iNaturalist.

    Args:
        place_id: iNaturalist place ID (e.g., 14 for California)
        taxon_groups: list of group keys from TAXON_GROUPS (e.g., ['birds', 'mammals'])
        progress_callback: optional callable(message, current=None, total=None)

    Returns:
        list of species common names
    """
    all_species = []

    for gi, group_key in enumerate(taxon_groups):
        group = TAXON_GROUPS.get(group_key)
        if not group:
            continue

        taxon_id = group['id']
        group_name = group['name']
        group_prefix = f"[{gi + 1}/{len(taxon_groups)}] {group_name}"

        if progress_callback:
            progress_callback(f"{group_prefix}: starting...", 0, 0)

        page = 1
        per_page = 500
        group_species = []
        group_total = 0

        while True:
            params = urllib.parse.urlencode({
                'place_id': place_id,
                'taxon_id': taxon_id,
                'per_page': per_page,
                'page': page,
            })
            url = f"{INAT_API}/observations/species_counts?{params}"

            data = None
            for attempt in range(3):
                try:
                    req = urllib.request.Request(url, headers={'User-Agent': 'Spotter/1.0'})
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        data = json.loads(resp.read())
                    break
                except Exception:
                    log.warning("Fetch attempt %d failed for page %d of %s",
                                attempt + 1, page, group_name, exc_info=True)
                    if attempt < 2:
                        import time
                        time.sleep(2)
                    else:
                        if progress_callback:
                            progress_callback(f"{group_prefix}: failed after 3 attempts on page {page}", 0, 0)

            if data is None:
                break

            results = data.get('results', [])
            if not results:
                break

            group_total = data.get('total_results', 0)

            for r in results:
                taxon = r.get('taxon', {})
                common_name = taxon.get('preferred_common_name', '')
                scientific_name = taxon.get('name', '')
                name = common_name or scientific_name
                if name:
                    group_species.append(name)

            fetched = (page - 1) * per_page + len(results)

            if progress_callback:
                progress_callback(
                    f"{group_prefix}: {fetched}/{group_total} species",
                    fetched, group_total,
                )

            if fetched >= group_total:
                break
            page += 1

        log.info("Fetched %d %s species for place %d", len(group_species), group_name, place_id)
        all_species.extend(group_species)

    if progress_callback:
        progress_callback(f"Done — {len(all_species)} total species", len(all_species), len(all_species))

    return all_species


def save_labels(name, place_id, place_name, taxon_groups, species):
    """Save a labels list to disk.

    Args:
        name: label set name (e.g., "California Birds")
        place_id: iNaturalist place ID
        place_name: human-readable place name
        taxon_groups: list of group keys used
        species: list of species names

    Returns:
        path to saved labels file
    """
    os.makedirs(LABELS_DIR, exist_ok=True)

    slug = name.lower().replace(' ', '-')
    labels_path = os.path.join(LABELS_DIR, f"{slug}.txt")
    meta_path = os.path.join(LABELS_DIR, f"{slug}.json")

    # Write labels file (one per line)
    with open(labels_path, 'w') as f:
        for sp in sorted(set(species)):
            f.write(sp + '\n')

    # Write metadata
    meta = {
        'name': name,
        'place_id': place_id,
        'place_name': place_name,
        'taxon_groups': taxon_groups,
        'species_count': len(set(species)),
        'labels_file': labels_path,
    }
    with open(meta_path, 'w') as f:
        json.dump(meta, f, indent=2)

    return labels_path


def get_saved_labels():
    """List all saved label sets.

    Returns:
        list of {name, place_name, taxon_groups, species_count, labels_file}
    """
    if not os.path.isdir(LABELS_DIR):
        return []

    result = []
    for fname in sorted(os.listdir(LABELS_DIR)):
        if fname.endswith('.json'):
            try:
                with open(os.path.join(LABELS_DIR, fname)) as f:
                    meta = json.load(f)
                result.append(meta)
            except Exception:
                pass
    return result


def get_active_labels():
    """Return the currently active labels file path, or None."""
    config_path = os.path.expanduser("~/.spotter/labels_active.json")
    if os.path.exists(config_path):
        try:
            with open(config_path) as f:
                data = json.load(f)
            path = data.get('labels_file', '')
            if path and os.path.exists(path):
                return data
        except Exception:
            pass
    return None


def set_active_labels(labels_file):
    """Set the active labels file."""
    config_path = os.path.expanduser("~/.spotter/labels_active.json")
    # Find the metadata for this labels file
    for saved in get_saved_labels():
        if saved.get('labels_file') == labels_file:
            with open(config_path, 'w') as f:
                json.dump(saved, f, indent=2)
            return
    # Fallback — save just the path
    with open(config_path, 'w') as f:
        json.dump({'labels_file': labels_file}, f, indent=2)
