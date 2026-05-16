import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vireo"))

from compare import compare_prediction_to_keywords


class FakeTaxonomy:
    taxa = {
        "sparrow": {
            "scientific_name": "Passerellidae",
            "lineage_names": ["Animalia", "Chordata", "Aves"],
            "lineage_ranks": ["kingdom", "phylum", "class"],
        },
        "white-crowned sparrow": {
            "scientific_name": "Zonotrichia leucophrys",
            "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Passerellidae", "Zonotrichia"],
            "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus"],
        },
        "golden-crowned sparrow": {
            "scientific_name": "Zonotrichia atricapilla",
            "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Passerellidae", "Zonotrichia"],
            "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus"],
        },
        "red-tailed hawk": {
            "scientific_name": "Buteo jamaicensis",
            "lineage_names": ["Animalia", "Chordata", "Aves", "Accipitriformes", "Accipitridae", "Buteo"],
            "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus"],
        },
    }

    def lookup(self, name):
        return self.taxa.get(name.lower())

    def relationship(self, name_a, name_b):
        a = self.lookup(name_a)
        b = self.lookup(name_b)
        if not a or not b:
            return None
        sci_a = a["scientific_name"].lower()
        sci_b = b["scientific_name"].lower()
        if sci_a == sci_b:
            return "same"
        if sci_a in [n.lower() for n in b["lineage_names"]]:
            return "ancestor"
        if sci_b in [n.lower() for n in a["lineage_names"]]:
            return "descendant"
        if a["lineage_names"][-1] == b["lineage_names"][-1]:
            return "sibling"
        return "unrelated"


def test_compare_prediction_to_keywords_refinement():
    result = compare_prediction_to_keywords(
        "White-crowned Sparrow",
        ["Sparrow"],
        FakeTaxonomy(),
    )

    assert result["category"] == "refinement"
    assert result["matched_keyword"] == "Sparrow"


def test_compare_prediction_to_keywords_broader():
    result = compare_prediction_to_keywords(
        "Sparrow",
        ["White-crowned Sparrow"],
        FakeTaxonomy(),
    )

    assert result["category"] == "broader"


def test_compare_prediction_to_keywords_conflict_with_shared_rank():
    result = compare_prediction_to_keywords(
        "Golden-crowned Sparrow",
        ["White-crowned Sparrow"],
        FakeTaxonomy(),
    )

    assert result["category"] == "conflict"
    assert result["shared_rank"] == "genus"


def test_compare_prediction_to_keywords_new_without_species_keyword():
    result = compare_prediction_to_keywords(
        "Red-tailed Hawk",
        [],
        FakeTaxonomy(),
    )

    assert result["category"] == "new"
