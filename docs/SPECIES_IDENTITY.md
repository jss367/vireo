# Species identity and naming

Vireo retains the text used as a classifier prompt separately from the species
identity used for comparison and display. Two common names can refer to one
species, and the same common name can appear on different taxa.

Regional label downloads store the iNaturalist taxon ID, scientific name, and
rank in the label set's JSON metadata. The text file remains a list of prompts.
The metadata includes a digest of that text, so editing a prompt cannot silently
attach another label's old identity. Legacy text files continue to work without
source metadata. Conflicting source identities for the same prompt stop model
construction with a request to use distinct scientific names.
When merged label sets use two names for the same source taxon, Vireo keeps one
deterministically chosen prompt so the classifier does not split that species'
probability between duplicate classes.

Source taxon IDs are persisted in `predictions.source_taxon_id` and carried in
portable classifier artifacts. These are iNaturalist IDs, not local `taxa.id`
values. The original prediction text and confidence remain available. The shared
resolver uses explicit source identity first, model-native scientific names next,
and unambiguous taxonomy lookup for legacy common-name labels. Unresolved names
retain their text. Hybrids are not merged into either parent species.

Pipeline feature loading, cached Process Review names, culling, classifier burst
comparison, and ID Conflicts use this resolution. Cached names are refreshed on
read without rewriting confirmed keywords. Grouping fingerprints change when
resolution rules change, so an old encounter arrangement is marked outdated;
regrouping recomputes membership without rerunning image classification.

## Repair on upgrade

The upgrade includes a narrowly scoped correction for the old taxonomy mapping
from “Red-crowned Amazon” to *Amazona rhodocorytha*. The intended species is
*Amazona viridigenalis*, also called Red-crowned Parrot. The evidence is
[Cornell's species account](https://birdnet.cornell.edu/taxonomy/species/Amazona%20viridigenalis)
and iNaturalist taxon 18976.

On the first normal database initialization after upgrade, Vireo repairs matching
legacy custom-label BioCLIP predictions. It preserves prediction IDs, raw labels,
confidence, keywords, and every workspace's review decisions. Fixed-head outputs,
source-backed predictions, hybrids, and other ambiguous historical mismatches
are excluded. This is not a general replacement of “Amazon” with “Parrot.”

Every changed row has a before/after record in `species_identity_repairs`, including
the reason, resolution version digest, and time. The repair is transactional and
idempotent. A preview is available programmatically through
`species_identity_repair.plan_repairs(connection)` on an initialized schema;
it performs only SELECT queries.

To inspect the audit:

```sql
SELECT prediction_id, before_json, after_json, reason, repaired_at
FROM species_identity_repairs
ORDER BY id;
```

Classifier cache identity includes both the label source metadata and the species
resolution policy. Old portable artifacts cannot satisfy the new runtime identity.
Text-embedding identity still depends on the prompt strings, so changing taxonomy
metadata does not require recomputing those embeddings. Corrected historical runs
are marked legacy until republished or classified under the new runtime.

Older downloaded taxonomy files and database name indexes did not retain all
alternate-name collisions. Until taxonomy is downloaded and imported again,
those unverified common names retain their raw text instead of inferring a taxon;
explicit source IDs, scientific names, and the verified red-crowned correction
continue to resolve. Downloads carry a common-name identity format version, and
imports preserve the ambiguity exclusions for database-backed review and culling.

Pipeline prediction tuples retain their identity key alongside display name,
confidence, and model through serialization and cache refresh. Encounter scoring,
consensus, rarity protection, and culling use that key. If an explicit source ID
has no scientific name in the catalog, review labels include the taxon number so
different unresolved taxa with the same common name remain distinguishable.

Accepting a source-backed prediction retains its iNaturalist ID on the keyword
and binds the local taxon directly. Existing keywords for another taxon are not
reassigned; a distinct name is used when necessary. IDs missing from the local
taxonomy remain on the keyword and are linked after a later taxonomy import,
without renaming already confirmed keywords. Label-set metadata updates for the
same source ID do not make the prompt ambiguous.

Process Review compares each prediction's identity key, and confirmed keywords
carry their own keys into both review views. Different taxa that share a display
name remain separate in conflict evidence and mixed-species detection.

## Label lists saved before identities

Lists downloaded before label sets recorded taxon IDs hold names only. Their
BioCLIP predictions are identified by name lookup, so a name the taxonomy
cannot assign to one taxon stays unresolved. For example, "Redhead" is also an
alternate English name of the Common Pochard, so that label and the iNat21
prediction of the same bird appear as two review rows. Preferring
iNaturalist's first-listed English name does not fix this: "Terciopelo" is
listed first for a plant and as an alternate for the fer-de-lance.

At startup, Vireo re-runs each legacy list's own iNaturalist query (its saved
place, taxon groups and observation filter) and identifies a prompt only when
exactly one fetched taxon has that name. Before that, it rebuilds the label
set from its source files and checks that it still matches the recorded
fingerprint; a list edited or re-downloaded since classification is not
matched. Identities are stored in `label_source_identities` under the label
set's fingerprint, not written into the list. Writing them into the list would
change the fingerprint and mark every photo classified with it as needing
reclassification. Existing predictions are stamped with `source_taxon_id` and
the source binomial, and each change is recorded in `species_identity_repairs`
with reason `label-list-source-identity`. Triggers on `predictions` stamp
rows written later under that fingerprint. The run appears in the bottom panel
as "Label List Species Ids". A list that cannot be reached is reported and
retried at the next startup.

Predictions older than label fingerprints use consensus across all saved lists,
including legacy lists that have no newer predictions. Those lists are queried
using their own provenance before consensus is marked complete. Any lookup
failure defers this pass and leaves it retryable. Case-only prompt variants
participate in the same conflict check; agreeing sources identify the prompt,
and conflicting source taxa leave it unresolved.

The legacy sentinel did not retain classifier mode, so consensus is limited
to unenriched BioCLIP text rows. Existing scientific names or taxonomy ranks
may be native Tree-of-Life evidence and are preserved. Legacy-only consensus
also snapshots source text and sidecars and rechecks them inside its writer
transaction; changed evidence leaves the pass unmarked for retry.
