"""Read XMP keywords and compare against model predictions."""

import logging
from xml.etree import ElementTree as ET
from pathlib import Path

log = logging.getLogger(__name__)

NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_DC = "http://purl.org/dc/elements/1.1/"


def read_xmp_keywords(xmp_path):
    """Read dc:subject keywords from an XMP sidecar file.

    Args:
        xmp_path: path to .xmp file

    Returns:
        set of keyword strings (empty if file missing or corrupt)
    """
    path = Path(xmp_path)
    if not path.exists():
        return set()

    try:
        tree = ET.parse(path)
    except ET.ParseError:
        log.warning("Corrupt XMP file: %s", xmp_path)
        return set()

    root = tree.getroot()
    keywords = set()
    for li in root.findall(f".//{{{NS_DC}}}subject/{{{NS_RDF}}}Bag/{{{NS_RDF}}}li"):
        if li.text:
            keywords.add(li.text)
    return keywords


def categorize(prediction, existing_keywords, labels_vocab):
    """Categorize a prediction relative to existing keywords.

    Args:
        prediction: the model's predicted species name
        existing_keywords: set of all dc:subject keywords from the XMP
        labels_vocab: set of known species labels (used to filter
                      existing keywords to just species, ignoring locations etc.)

    Returns:
        'match' — prediction matches an existing species keyword
        'new' — no existing species keywords found
        'refinement' — prediction is more specific than an existing keyword
        'disagreement' — prediction differs from existing species keyword
    """
    pred_lower = prediction.lower()

    # Filter existing keywords to just species (those in the labels vocab)
    existing_species = set()
    for kw in existing_keywords:
        for label in labels_vocab:
            if kw.lower() == label.lower():
                existing_species.add(kw)
                break

    # No species keywords exist — this is new info
    if not existing_species:
        return 'new'

    # Check for exact match (case-insensitive)
    for sp in existing_species:
        if sp.lower() == pred_lower:
            return 'match'

    # Check for refinement: existing keyword is a substring of prediction
    # or they share a significant word (e.g., "hawk" in "Red-tailed hawk")
    for sp in existing_species:
        sp_lower = sp.lower()
        if sp_lower in pred_lower or pred_lower in sp_lower:
            return 'refinement'
        # Check shared words (ignoring short words)
        sp_words = {w for w in sp_lower.replace('-', ' ').split() if len(w) > 2}
        pred_words = {w for w in pred_lower.replace('-', ' ').split() if len(w) > 2}
        if sp_words & pred_words:
            return 'refinement'

    return 'disagreement'
