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


def categorize(prediction, existing_keywords, taxonomy):
    """Categorize a prediction relative to existing keywords using taxonomy.

    Args:
        prediction: the model's predicted species name
        existing_keywords: set of all dc:subject keywords from the XMP
        taxonomy: a Taxonomy instance for looking up taxa and relationships

    Returns:
        'match' — prediction matches an existing species keyword
        'new' — no existing species keywords found
        'refinement' — prediction is more specific than an existing keyword
        'disagreement' — prediction differs from existing species keyword
    """
    # Filter existing keywords to just those recognized as taxa
    existing_taxa = []
    for kw in existing_keywords:
        if taxonomy.is_taxon(kw):
            existing_taxa.append(kw)
        else:
            log.debug("Ignoring non-taxon keyword: %s", kw)

    # No recognized taxa in existing keywords — this is new info
    if not existing_taxa:
        return 'new'

    # Check each existing taxon against the prediction
    for taxon_kw in existing_taxa:
        rel = taxonomy.relationship(taxon_kw, prediction)

        if rel is None:
            log.warning("Prediction '%s' not found in taxonomy", prediction)
            return 'disagreement'
        elif rel == 'same':
            return 'match'
        elif rel == 'ancestor':
            # Existing is broader, prediction is more specific → refinement
            return 'refinement'
        elif rel == 'descendant':
            # Existing is more specific, prediction is broader — unusual but treat as match
            return 'match'
        # 'sibling' and 'unrelated' fall through to check remaining taxa

    # No existing taxon matched/contained the prediction
    return 'disagreement'
