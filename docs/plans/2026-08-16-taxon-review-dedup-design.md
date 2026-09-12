# Taxon-keyed Review deduplication — design

**Date:** 2026-08-16
**Status:** Proposed
**Motivating bug:** Two classifiers (BioCLIP-2.5 and iNat21) classified the same
8-photo burst as "Eurasian Blue Tit" and "Blue Tit". Both labels mean
*Cyanistes caeruleus*, but Review shows two cards for the same eight photos,
and accepting one leaves the other pending as a hidden duplicate.

## Problem

Three independent mechanisms conspire to produce duplicate cards:

1. **Burst group IDs are minted per classify job** —
   `gid = f"g{job_id[-6:]}-{group_count:04d}"` in
   `classify_job.py::_store_grouped_predictions`. Two models classifying the
   same burst always produce two distinct `group_id`s.
2. **Review deduplicates client-side by `group_id` only** —
   `review.html::getVisibleItems` keeps the first prediction per `group_id`.
   Nothing compares cards across groups.
3. **Cross-model agreement is matched by string equality** — the one place
   that resolves agreeing models together, `db.py::accept_subject_species`
   (used by Compare), matches siblings with
   `lower(trim(pr.species)) = lower(trim(?))`. "Blue Tit" ≠
   "Eurasian Blue Tit", so even that path would not have merged this pair.

A hard-coded alias ("Eurasian Blue Tit" → "Blue Tit") would fix one species
and recur for every regional common name. The durable fix is to compare
predictions by **taxon**, not by label string.

## Goals

- One Review card per (species claim, burst/subject), regardless of how many
  classifier models produced it or which common-name variant each used.
- Accepting or rejecting that card resolves **all** agreeing model rows — no
  hidden pending duplicates.
- A card, its detail view and its actions never cover more than the user
  could see, and the card's status never hides a member's — §2's *scope
  invariant*. Where a filter forces this against the goal above (agreeing
  rows the user cannot see, or agreeing rows shown as two separate
  cards), this one wins and the merge is deferred to the unfiltered
  view.
- Both models' outputs stay visible on the merged card (model, confidence,
  votes) — per CORE_PHILOSOPHY, the merge must not hide what each model said.
- Works retroactively on existing prediction rows with no destructive
  migration.
- Genuine disagreements (different taxa) keep separate cards. This feature
  only merges *agreement*.

## Non-goals

- Changing burst grouping itself (timestamp windows, similarity refinement).
- Retroactively renaming existing keywords ("Eurasian Blue Tit" keywords on
  disk stay as they are; see §4 for how new accepts avoid fragmentation).
- Merging across taxonomic ranks (a genus-level "Cyanistes" claim does not
  merge with a species-level claim). Strict taxon equality only.
- Cross-taxonomy synonym resolution beyond what the local taxa tables and the
  cached iNat alternate-name lookup provide.

## Design

Four parts: (1) a canonical taxon key per prediction, (2) server-side card
building in `/api/predictions`, (3) taxon-keyed cross-model accept/reject,
(4) display-name and keyword canonicalization.

### 1. Canonical taxon key

New helper (in `taxonomy.py`, usable from both `app.py` and `db.py`):

```python
def taxon_key_for(label, scientific_names, tax):
    """Return a canonical merge key for a *label*, not for one row.

    ('taxon', taxon_id)      when the label resolves in the taxonomy
    ('name', folded_string)  fallback — merges only identical labels

    ``label`` is a folded species key; ``scientific_names`` is the set of
    distinct non-NULL scientific names carried by that label's rows. See
    "One key per label, not per row" below for why the second argument is
    a set and not a row's column.

    If two members of ``scientific_names`` resolve to different taxa the
    ladder is not run at all: the return is the ``('name', label)``
    fallback. See "Conflicts fall to the name key, not to the next rung".
    """
```

Resolution ladder:

1. `predictions.scientific_name` → `Taxonomy.lookup()` (the `_by_scientific`
   index). This column is populated at classify time from the label set's own
   taxonomy metadata, so it is the most reliable signal when present. It is
   also the only rung that reads a *per-row* column, which is why it is the
   one the canonicalization rule below has to constrain.
2. `predictions.species` → `Taxonomy.lookup()` (common name, scientific name,
   punctuation-normalized common name).
3. `predictions.species` → **cached** `Taxonomy.api_lookup` result only —
   i.e., the request path reads the persistent iNat-lookup cache but must
   never issue a live HTTP request. iNat's autocomplete matches
   *alternate/regional* common names ("Eurasian Blue Tit" is precisely an
   alternate name for *Cyanistes caeruleus*), so cached hits let the merge
   work; cache misses degrade to step 4 until a background resolver (below)
   populates the cache.
4. Fallback: `('name', ascii_folded(species))` using the same folding rules
   as `_folded_species_key` in `classify_job.py`. Unresolvable labels
   (custom label files, informal groups like "gull sp.") merge only when the
   strings are identical — i.e., current behavior is preserved for them.

**No live network I/O in `/api/predictions`.** `Taxonomy.api_lookup`'s
existing 10-second HTTP timeout and its silent-on-failure behavior (a
connection error returns without adding the label to `_api_misses`, so a
firewalled host would retry per-refresh) are the reason: calling it inline
would let one offline install stall Review for 10s per unresolved label on
every load. Resolution therefore splits in two:

- **Request path (§1 above):** cache-only reads. `taxon_key_for` calls a new
  `Taxonomy.cached_api_lookup(label)` that returns a hit from the persistent
  cache, a sentinel for a cached negative, or `None` for "unknown, ask the
  resolver". Never issues an HTTP request; never blocks.
- **Background resolver:** unresolved labels are enqueued (a) at classify
  time when `_store_grouped_predictions` first stores a row whose
  `species`/`scientific_name` don't hit steps 1–2, and (b) opportunistically
  when `/api/predictions` observes a `name:` fallback for a label it has
  not seen. A single-flight background job in `jobs.py`
  (`resolve_taxonomy_labels`) drains the queue, calls `api_lookup` with the
  existing 10s timeout, and **persists both hits and misses** — extending
  `Taxonomy._api_misses` to a bounded on-disk negative cache (label →
  {resolved_at, retry_after}) with an exponential-then-daily retry so a
  transient outage doesn't pin a label as unresolvable forever, and so an
  offline host never re-hits the network on refresh. The Review card for a
  still-unresolved label simply doesn't merge until the resolver succeeds
  and the next refresh sees the cache entry.

Rules:

- A `('name', …)` key never merges with a `('taxon', …)` key, even if one is
  a substring of the other. No guessing.
- `NULL`/empty species never merges with anything.
- Rank is respected implicitly: genus and species resolve to different
  `taxon_id`s, so they never merge.

**One key per label, not per row.** Steps 2–4 are pure functions of the
label: any two rows spelling the same species get the same answer out of
them. Step 1 is not. `predictions.scientific_name` is a per-row column,
and two rows carrying the *same* label can disagree on it — not as a
corner case, but as the ordinary consequence of it being the strongest
rung:

- Step 1 reads what the **classifier's own label metadata** supplied at
  write time (`item["taxonomy"]`, `classify_job.py:1313, 1327`), which is
  a different information source from the read-time `Taxonomy` that steps
  2–3 consult. Step 1 hitting while step 2 misses *is* the
  alternate-common-name case this design exists to merge.
- Stored rows are never refreshed. The non-`reclassify` reuse path
  re-injects cached rows with `"taxonomy": None` (`classify_job.py:1577,
  1709, 2468`) and `_store_pending_detection_prediction` returns early
  for them after updating only group metadata
  (`classify_job.py:2061-2099`); `add_prediction`'s `INSERT OR IGNORE`
  (`db.py:15862`) could not have rewritten the columns anyway. A row's
  `scientific_name` is whatever the run that *first* inserted it could
  see.
- What a run could see varies between runs. `load_local_taxonomy`
  returns `None` when no taxonomy file is installed
  (`taxonomy.py:739-758`) and `get_hierarchy` returns `{}` for a label
  the file does not carry (`taxonomy.py:968-987`), in which case
  `add_prediction` stores `scientific_name = NULL`
  (`db.py:15867-15883`). Installing or updating `taxonomy.json` between
  two runs changes the value stored for new rows and leaves old rows
  alone — and does not change `labels_fingerprint`, which hashes the
  label strings and nothing else (`labels_fingerprint.py:15-23`), so the
  divergent rows stay in the same bucket.

So one burst — the ordinary cached-plus-new burst of §2 — can hold a row
that keys `taxon:13094` off its stored scientific name next to a row that
keys `name:eurasian blue tit` off the identical label. The merge graph
requires exactly one key per node, so a per-row ladder cannot be the
input to it.

The key is therefore resolved **per label, over the request's row set,
before the graph is built**:

1. Group the request's rows by folded species key `L` (the same
   `_species_match_key` the node key's fourth element uses). Steps 2–3
   run on one canonical spelling of `L` — the lexicographically smallest
   distinct `_folded_species_key` spelling among those rows — so two
   frames that differ only by apostrophe or case cannot reach
   `Taxonomy.lookup` as two different strings.
2. Collect `S(L)`, the distinct non-`NULL` folded `scientific_name`
   values on those rows, and resolve each through `Taxonomy.lookup`. If
   one or more resolve and they all resolve to the **same** `taxon_id`,
   step 1 yields `('taxon', that_id)` for every row labelled `L`.
3. If two elements of `S(L)` resolve to **different** `taxon_id`s, that
   is a genuine conflict — a taxonomic split or lump recorded at two
   different write times — and the ladder is **abandoned for `L`
   entirely**. Its key is the rung-4 fallback, `('name', L)`, computed
   on the canonical folded spelling chosen in step 1. Rungs 2 and 3 are
   not consulted. No tie-break and no arbitrary winner: choosing one of
   two real taxa by lexical order is exactly the guessing the rules
   above forbid.

**Conflicts fall to the name key, not to the next rung.** Skipping only
rung 1 and letting the ladder continue would look like a conservative
degradation and would not be one. Rungs 2 and 3 key off the *label*,
which is single-valued by construction, so they resolve happily and
return some `('taxon', X)` — and a taxon key is precisely the thing that
merges across *different* labels. The conflict just detected would then
be silently resolved after all, in favour of whichever taxon the
common-name lookup (or its cached iNat alternate) happens to prefer, and
every row labelled `L` would merge with alternate-label rows for `X`.
Accepting that card would canonicalize and keyword predictions whose own
strongest stored metadata names the other taxon — the exact failure the
conflict detection exists to prevent, reintroduced one rung lower.

Only the `name:` key actually declines to choose. Two properties of it,
both already stated in the rules above, are what make the conflicting
label's rows stay put:

- `('name', L)` never merges with any `('taxon', …)` key, so no
  alternate-label node for either candidate taxon can attach to `L`'s
  nodes, however the taxonomy resolves those labels.
- `('name', L)` merges with another `name:` key only on string equality,
  so `L`'s nodes still merge with each other across models and bursts —
  the same-spelling merge Review already does today.

So a conflicting label's component is exactly the set of nodes spelling
`L`, no wider: identical-label rows still collapse into one card, and
every differently-spelled node stays a separate card, which is today's
behavior for that label rather than a wrong merge. `mixed` badges,
frozen membership and the sibling scan all operate on that narrower
component unchanged — nothing else in the design needs a special case,
because a conflicting label produces an ordinary `name:`-keyed card.

The cost is bounded and visible: a label with genuinely contradictory
stored scientific names loses cross-label merging until the underlying
disagreement is fixed (by a reclassify, which rewrites
`scientific_name`). It does not lose same-label merging, and it never
produces a card that spans two taxa.

The invariant this buys is stronger than "one key per node", and it is
what the rest of the design leans on:

> The taxon key is a function of the folded species label and of the
> request's rows for that label — never of an individual row.

One key per label implies one key per node, because the node key contains
the label (§2, "Node identity"), and therefore one key per card
component. It also closes the same divergence *across* nodes, which a
node-local canonicalization would not: two models' views of one burst,
one carrying stored scientific names and one not, resolve alike and
therefore still merge — the motivating case of the whole design.

`S(L)` is read off rows the request already has in hand, so it costs no
extra query, and the mutation path recomputes it over the same scoped row
set before rebuilding the graph (§2, "Anchor lookup and cache-transition
safety", step 3). A GET and its POST therefore cannot disagree about a
card's key for any reason other than a taxonomy-cache transition — which
that section already handles, and which the frozen membership already
bounds.

**No schema change to `predictions` for the taxon key.** (§2's Phase 0
does add one additive nullable column, `run_token`, for edge
suppression — guarded by a `db_meta` marker exactly as this bullet's
escape-hatch note prescribes, and with no backfill. Nothing about the
taxon key or node identity depends on it.) The key is computed at read time
from the in-memory taxonomy dict (O(1) per row) plus the cached iNat lookup
table. The background resolver's negative cache is stored in the existing
`taxonomy_api_cache` mechanism (or a peer table with the same lifecycle) —
additive rows, not a schema migration on hot tables. If profiling later
shows cost on large Review payloads even with a cold cache, an additive
nullable `predictions.taxon_id` column with lazy write-back is the escape
hatch — explicitly deferred (solo-user DB, easy to add later; see also the
`user_version` drift caveat in memory — any future column should be guarded
by a `db_meta` marker or a PRAGMA column check, not a version-gated
migration).

### 2. Server-side card building in `/api/predictions`

Today the client dedups by `group_id`, which cannot see cross-model
duplicates. Move card identity to the server, where the taxonomy is
available, and where the accept path needs the same key anyway.

`api_predictions` (app.py:15278) computes for each returned prediction:

- `taxon_key` — from §1, serialized as e.g. `"taxon:13094"` or
  `"name:blue tit"`. Stamped on every row, but *resolved* once per
  folded label over the whole returned row set (§1, "One key per label,
  not per row") before any graph work begins — so two rows sharing a
  label always carry byte-identical `taxon_key`s, whatever their
  individual `scientific_name` columns say.
- `card_id` — the merge unit, computed as follows.

**Merge rule.** Build a graph whose nodes are burst groups plus singleton
predictions (no group). Connect two nodes when they have the **same
`taxon_key`** and their **photo memberships intersect**. Each connected
component is one card; `card_id` is derived from the lexicographically
smallest member's stable node key alone (see "Card ID encoding" below).
The taxon key is *not* baked into `card_id` — it is recomputed from the
anchor node's rows on the mutation POST, so cards survive taxonomy-cache
transitions between the GET and the click (see "Anchor lookup and
cache-transition safety" below).

**Node identity.** The graph keys each burst-group node as
`(classifier_model, labels_fingerprint, group_id)`. That tuple is
collision-resistant *only if* the raw `group_id` is itself unique across
jobs sharing the same model and fingerprint — which today's
`_store_grouped_predictions` scheme
(`f"g{job_id[-6:]}-{group_count:04d}"`, `classify_job.py:2212`) cannot
guarantee: two same-model, same-fingerprint jobs whose truncated
`job_id[-6:]` suffix and per-job group counter both align mint identical
`group_id`s for disjoint photo sets. Two such bursts would occupy one
graph node and become one card without any same-taxon overlap edge,
contradicting the goal.

The read side cannot recover the *original* job identity after the
fact: `predictions` has no `job_id` column, `add_prediction` does not
receive a job identifier, and `prediction_review` carries only the
plain `group_id` string (`db.py:865-883, 925-935, 15802-15818`). Only
the truncated 6-char suffix embedded inside `group_id` survives to
disk, so no merge-time key over stored rows can recover the full job
identity without a schema change and a companion backfill — which
would violate the "works retroactively on existing prediction rows
with no destructive migration" goal. Phase 0's `run_token` column is
not that backfill and does not contradict this: it is additive,
nullable and written only *going forward*, so existing rows keep
exactly today's behaviour through the `legacy:` run-key namespace (§2,
"Run key") rather than being rewritten with a guessed job identity.
What is unrecoverable stays unrecovered; what is knowable from here on
is recorded.

Without *some* split, two independently-minted bursts that collide on
`(classifier_model, labels_fingerprint, group_id)` share one graph
node; the same-taxon overlap edge test only connects *distinct* nodes,
so they could not be separated by any downstream rule and would merge
into one card even with no shared photos. The question is what signal
a read-time split can legitimately use.

A photo-membership partition does not work: each `predictions` row
has exactly one `photo_id` (`db.py:865-883`), and an ordinary
multi-photo burst has one row per distinct photo, so *no two rows in
one burst share a photo*. A "rows share a photo iff their `photo_id`s
coincide" rule would shatter every normal burst into single-photo
subsets and render one card per frame instead of one card for the
burst.

**Neither write time nor capture time can partition a legacy bucket.**
Two earlier revisions of this design tried to split a colliding bucket
by a time signal. Both are wrong, and the read path uses neither.

*Write time (`predictions.created_at`) — rejected.* The theory was
that one `_store_grouped_predictions` call writes its rows in a single
transaction, so a job's rows are seconds apart while two colliding
jobs are minutes-to-days apart (`db.py:881`, `TEXT DEFAULT
(datetime('now'))`). That theory is wrong:

- On a non-`reclassify` run, the gated branch re-injects *cached*
  predictions into `raw_results` (`classify_job.py:1657-1712`,
  `_existing: True`) so grouping still sees those photos. A single
  burst can therefore be composed of some photos classified today and
  some whose rows were written weeks ago.
- `add_prediction` uses `INSERT OR IGNORE` (`db.py:15862`) against
  `UNIQUE(detection_id, classifier_model, labels_fingerprint,
  species)`, so re-storing a cached row is a no-op and the row keeps
  its **original** `created_at` even though
  `_store_grouped_predictions` just assigned the whole burst a fresh
  `group_id`.
- Nothing records when the grouping assignment happened:
  `prediction_review` (`db.py:925-935`) has `group_id` but no
  creation timestamp, and its `reviewed_at` is `NULL` while pending.

So `created_at` is the *first-ever* insert time of each row, not a run
boundary, and a perfectly ordinary mixed cached/new burst would be
shredded into several nodes and several cards by a `created_at` gap
rule.

*Capture time (`photos.timestamp`) — also rejected.* The theory here
was better: the grouper itself walks capture timestamps
(`group_by_timestamp`, `grouping.py:12-55`), so a stored burst should
be capture-time-connected by construction, and a read-side gap rule
whose window `W_read` is at least as wide as any window the grouper
could legally have used (the schema max, `3600`,
`vireo/config_schema.py:89-93`) could never shatter a real burst. It
is still wrong, for two independent reasons:

1. **Stored bursts are not capture-time-connected.**
   `_store_grouped_predictions` does not store `group_by_timestamp`'s
   output. It stores the output of `refine_groups_by_similarity`
   (`classify_job.py:2170-2172`), which re-partitions each timestamp
   group by embedding similarity: a photo joins the first subgroup
   holding *any* member it is similar to, so intervening photos can
   land in a different subgroup (`grouping.py:83-119`). A stored burst
   is one of those subgroups, and its adjacent members can be
   arbitrarily far apart in capture time — bounded by
   `(n-1) × grouping_window`, not by `grouping_window`. Concretely at
   the schema-max window: photos at t=0, t=3500 and t=7000 form one
   timestamp group; if the first and last are visually similar and the
   middle one is not, the stored burst is {0, 7000}, and a gap rule
   with `W_read = 3600` splits one legitimate burst into two cards.
   Widening `W_read` does not fix it — the bound grows with burst
   length, not with the window — and it is the *common* path
   (similarity refinement runs on every classify job), not a legacy
   corner.
2. **Capture time is mutable, and the failure is not safe.**
   `_refresh_photo_metadata` (`vireo/capture_time.py:265-283`) and
   scanner refreshes re-read EXIF and update `photos.timestamp`, so a
   partition derived from it is not a function of immutable stored
   state. A correction landing between a Review GET and the mutation
   POST can *merge* two subsets, and the merge direction does not fail
   closed: if subsets A and B merge and A held the lower minimum
   `predictions.id`, the merged subset keeps A's anchor, so a POST
   carrying A's stale handle still resolves — and now mutates B's
   previously-separate, previously-hidden rows. Only a request anchored
   on B becomes unrecognized and returns 400. An earlier revision
   asserted the failure mode was "a 400, never a silent mismerge"; that
   held only for the *split* direction, and a safe-failure argument
   that covers one direction of a boundary change is not a
   safe-failure argument.

Together these rule out any read-time partition keyed on a photo
timestamp, so the design stops trying to reconstruct run boundaries
from time at all.

**Node identity is a pure function of immutable row columns.** The
graph keys each burst-group node as `(classifier_model,
labels_fingerprint, group_id, species_key)`, where `species_key` is
the ASCII-folded match key of `predictions.species` — the same folding
§1 step 4 uses (`_folded_species_key` / `_species_match_key` in
`classify_job.py`). Every element is a column already on the row.
Nothing is derived from a timestamp, from write order, or from which
rows the query returned.

Splitting a bucket by species key is free for real bursts, and it is a
structural guarantee rather than an assumption:
`_store_grouped_predictions` stamps a `group_id` only when
`group_reviewable` — `len({_species_match_key(p) for p in group}) == 1`
(`classify_job.py:2269-2272`); a burst whose frames disagree on species
is stored with `group_id=None` and its rows become singleton nodes.
**Every stored burst is therefore unanimous in species match key by
construction**, and always yields exactly one node no matter how many
species-string variants (casing, apostrophes) its individual frames
spell. That is the property the capture-time rule claimed and did not
actually have.

*What unanimity does and does not guarantee.* `group_reviewable` and the
node key's fourth element are the **same function** — `_species_match_key`
— so node identity and the write-time unanimity test agree exactly, and
have since round 2. But both are keyed on the species *label*, and
neither says anything about `predictions.scientific_name`, which is a
per-row column the reuse path never refreshes. A unanimous burst can
therefore still hold rows whose stored scientific names disagree, and
§1's ladder reads that column at its first rung. One taxon key per node
is consequently **not** a property of the node key at all — it is
delivered by §1's "One key per label, not per row", which resolves the
key from the label plus the request's rows for that label and so returns
the same answer for every row of a node by construction. Everywhere below
that asserts a node has one taxon key is citing that rule, not this one.

Splitting the bucket a fifth way — on the stored `scientific_name` — was
considered and rejected; it would shatter exactly the cached-plus-new
burst that is the common path, and the shards could never re-merge (see
"Alternatives considered").

On the legacy surface, this separates two pre-Phase-0 bursts that
collided on `f"g{job_id[-6:]}-{group_count:04d}"` whenever they are
*different species* — the case where merging them is visibly wrong,
because one card would assert a taxon over photos the classifier
called something else. Two colliding bursts of the **same** species
stay one node and one card. That residual is:

- **not recoverable from stored rows** by any rule — the original job
  identity is gone (no `job_id` column) and no time signal
  reconstructs it (above);
- **not a regression** — `review.html` already dedups on `group_id`
  alone today, so those two bursts render as one card today, and the
  card shows their union of photos either way; and
- **closed prospectively by Phase 0**, after which `group_id` is
  unique on its own and every bucket holds exactly one burst.

Accepting that residual is the deliberate trade. The alternative — a
time-based partition — bought a fix for a rare, pre-existing,
visible-to-the-user legacy artifact at the price of shattering
ordinary similarity-refined bursts on the common path. Under-splitting
a legacy collision is invisible and unchanged from today;
over-splitting a real burst is a regression every user would see.

Over-splitting by species key is itself self-correcting rather than
harmful: if a bucket ever did hold two species-string variants of the
*same* taxon, the graph's same-taxon + overlapping-photos edge
re-merges the two nodes into one card. Node keying can only propose a
finer partition; the edge test decides the final card.

**Filter-invariant by construction, at zero query cost.** An earlier
revision computed its partition over the *unfiltered* bucket so that a
handle minted under one filter state would still resolve under
another, at the cost of an extra bucket-scoped fetch of
`(prediction_id, photo_id, timestamp)` per bucket. Per-row intrinsic
identity gets the same guarantee for free: there is no partition to
scope, all four fields are already on every row the endpoint selects,
and a `node_id` minted under any filter state decodes to the same node
under any other. Filters still hide *rows* exactly as §2 "Filter
semantics" specifies; they cannot move a node's identity. The server
stamps the encoded `node_id` on each returned row, so the client never
recomputes node identity itself.

**Phase 0 (new, prerequisite of §2)** mints **one run token per classify
pass** — one `_store_grouped_predictions` call, which is one job over
one `(classifier_model, labels_fingerprint)` (`classify_job.py:2493`,
`3536`; a job never loops over models) — and builds group IDs out of it:

```python
run_token = secrets.token_hex(16)        # once, at entry
gid = f"g{run_token}-{group_count:04d}"
```

128 bits per pass plus a counter that is unique within the pass;
collision probability across the entire history of classify runs is
effectively zero. The ID stays an opaque backwards-compatible string, so
read-side consumers are unchanged. Minting per *pass* rather than per
*group* is what makes the token double as a run identity (next
paragraph) at no extra cost; per-group tokens would have been equally
unique and told us nothing about which rows were written together. Two
weaker options are explicitly rejected:

- `f"g{job_id}-{group_count:04d}"` (the full job ID). `job_id` is
  **not** unique across jobs by construction: `JobRunner` builds it as
  `f"{job_type}-{int(time.time() * 1000)}-{seq}"` from an
  `_enqueue_counter` that is initialized to `0` on every process start
  (`jobs.py:112, 687-689`). The counter only separates jobs *within* a
  process; across restarts the sole separator is the wall clock, so a
  restart combined with a backward clock adjustment (NTP step, manual
  clock change, VM snapshot restore, a DB carried to another machine)
  can mint the same `job_id` for the same job type at the same
  sequence number — recreating exactly the collision Phase 0 exists to
  eliminate. A key that has to hold for the life of the catalog should
  not rest on wall-clock monotonicity across process restarts.
- `secrets.token_hex(4)` — 32 bits. Because
  `_store_grouped_predictions` resets `group_count` per job, the
  counter suffix is shared across every job, so IDs minted at a common
  counter value reach ~50% birthday-collision probability after
  roughly 77k draws. Not safe at catalog scale.

**The token is persisted per row, because `group_id` alone cannot carry
it.** Phase 0 adds one nullable column, `predictions.run_token TEXT`,
written with the pass's token by every row the pass stores
(`_store_pending_detection_prediction`, `_store_match_prediction` —
`classify_job.py:2046`, `1883`) and refreshed on the `_existing` reuse
branch (`classify_job.py:2061-2099`) in the same place that already
updates or clears the row's group metadata, so a row's token always
names **the pass whose grouping decision that row currently reflects**.
Migration: `ALTER TABLE predictions ADD COLUMN run_token TEXT`, guarded
by a `db_meta` marker rather than `user_version` (the live DB's
`user_version` has drifted ahead of the schema constant, so a
version-gated migration silently skips). No backfill: the pass that
wrote a pre-Phase-0 row left no durable trace to recover — the row
carries neither a job column nor a usable time signal (above).

The column exists because the group ID cannot cover **singleton** rows:
a singleton node keys on `"p" + prediction_id` and has no `group_id` at
all, and singleton-versus-grouped on the same photo is precisely one of
the within-pass splits the edge rule below has to respect. Every other
choice was worse: `classifier_runs` is keyed
`(detection_id, classifier_model, labels_fingerprint)` and is deleted
and rewritten by reclassify paths (`db.py:16090-16134`), so it cannot
be read as history; `prediction_review` would fit the workspace scope
but pending rows are deliberately absent from it ("absence == pending",
`db.py:15908-15917`), and materializing a row per pending prediction to
hold a token inverts that invariant for every consumer.

The node key stays `(classifier_model, labels_fingerprint, group_id,
species_key)` — `run_token` is **not** part of any key, and node
identity is still read entirely off columns that already exist, so a
node minted before Phase 0 and one minted after decode the same way.
Phase 0 makes the fourth element redundant for new rows (a self-unique
`group_id` already means one bucket is one burst) and leaves it doing
useful work only on pre-Phase-0 rows. Phase 0 lands before the
merge-graph work in Phase 3, so every row the merge graph reads with
the new semantics has a self-unique `group_id` and a run token.

Singleton nodes key on `(classifier_model, labels_fingerprint, "p" +
prediction_id)`; `prediction_id` is a unique primary key
(`db.py:866`), so this tuple is collision-resistant on its own without
depending on Phase 0.

Why overlap, not identical membership: the two models' burst groups for the
same event frequently differ by a frame or two — grouping runs per job, and
each model independently diverts frames to auto-accept based on the XMP
keywords present *when that model ran*. Requiring identical membership would
silently fail to merge most real duplicates (the 8-vs-7 case). Union
membership is safe because every member row asserts "these photos show taxon
X" — exactly the claim accepting the card applies, photo by photo.

Why connected components can't over-merge: an edge requires same taxon *and*
shared photos. Two different bursts of the same species on different photos
stay separate cards; two models' views of the same burst merge.

**Within-run subject partition is authoritative on the edge.** Same-taxon
overlap is a *cross-run* signal only: the edge is added between nodes A and B
iff they share a taxon key, their photo memberships overlap, **and** their
**run keys are disjoint** — never within a single
classify run. When one run produces two same-taxon nodes on the same photos
— a real Blue Tit box and a false-positive Blue Tit box on the same frame,
placed by similarity grouping in different `group_id`s (or one grouped and
one singleton) — that split is the classifier's own subject-partition
decision, computed from image evidence at classify time. Re-merging those
two nodes at Review time on `(photo, taxon)` alone would collapse two
distinct subjects into one card and make one accept-or-reject decision
cover them both, which is precisely the granularity loss §"Distinct
same-taxon subjects on one photo" flags. The within-run half of the
`(taxon_key, overlapping photos)` rule therefore only *appears* to apply;
it is a no-op because within-run same-node identity already means one node,
and within-run different-node identity means the run separated the subjects
and the graph does not re-join them. Across runs (disjoint run keys — which
covers a different classifier or label set, and equally two passes that
shared both) the run partitions are not comparable — one
pass's `group_id`s carry no information about the other's — so the
overlap-and-same-taxon edge is the strongest cross-run signal we have and
is used as designed. The residual **cross-run ambiguity** on a shared photo
where both runs produce ≥2 same-taxon subjects — which of the two BioCLIP
subjects on frame 5 does which of the two iNat21 subjects correspond to? —
is genuinely undecidable from `(photo, taxon)` alone; that photo is dropped
from the overlap-set for edge purposes (either side's multiplicity
suppresses it), and the cross-run merge either succeeds through some *other*
unambiguous frame in the same burst or fails to merge and the two remain
separate cards. See §Edge cases, *Distinct same-taxon subjects on one
photo*, for the concrete shape.

**Run key — what "the same run" means, and why it is not
`(classifier_model, labels_fingerprint)`.** A row's run key is, in order:
the token embedded in its `group_id` when it has one (Phase 0 mints
`g{run_token}-{counter}`, so the prefix *is* the pass that grouped it);
otherwise its `predictions.run_token`; otherwise — only for rows written
before Phase 0 — the synthetic `legacy:{classifier_model}:{labels_fingerprint}`.
A node's run-key set is the set over its member rows, and **the edge is
suppressed iff the two nodes' run-key sets intersect.**

`(classifier_model, labels_fingerprint)` is a *configuration*, not a run.
Classifying a newly imported folder with the same model and the same label
set as last week's folder is the ordinary case, not an exotic one, and a
reclassify pass over a subset is another; every one of those passes shares
the pair. Treating the pair as a run identity therefore suppresses edges
between nodes from genuinely different passes — two same-taxon groups that
overlap on a photo stay two cards forever, the exact duplicate this design
exists to collapse — and does so *undetectably*, because nothing recorded
which pass wrote what. That is why the token has to be minted per pass in
Phase 0 rather than per group: the fix is unavailable later if the rows
never carried it. The pair survives only as the **legacy namespace**, where
it reproduces today's behaviour on rows that predate the token and nothing
better exists; because the two namespaces are disjoint strings, a legacy row
and a stamped row are never "the same run", which is correct — a stamped row
was written after the legacy row's pass ended.

Intersection rather than equality, because a node's rows can carry two
tokens: a pass that re-saw some of a burst's detections and inserted others
leaves the reused rows re-stamped by the refresh above and the new rows
stamped on insert, and a node that spans a legacy row and a stamped one is
reachable the same way. Suppression is the conservative branch — it
preserves a split the classifier made — so any pass shared by both nodes is
enough to conclude that some pass separated them deliberately.

One residual, stated rather than hidden: `group_id` is workspace-scoped
while `predictions.run_token` is global, so a second workspace
re-classifying the same detections re-stamps the global token on rows whose
*other* workspace's partition predates it. Grouped nodes are unaffected —
they read the token out of their own workspace's `group_id`, which the same
pass wrote — so this reaches only singleton nodes, and its failure direction
is an allowed edge where suppression would have blocked one: a singleton
merges into a same-taxon overlapping card, the same granularity loss §Edge
cases, *Distinct same-taxon subjects on one photo* already documents for the
cross-run-ambiguity case, rather than a wrong card identity.

**Payload changes.** Each prediction row gains `taxon_key`, `card_id`,
`node_id` (the encoded node identity from "Node identity" above — the
handle the client echoes back: as the mutation target when a filter is
active, and as the card's frozen membership otherwise, §3 step 1), and
`display_name` (§4). Rows are *not* collapsed server-side — the client keeps
all rows (it already receives every group member) and dedups by `card_id`
instead of `group_id`, so per-model detail remains available for rendering.
`run_token` is added to the endpoint's column list because the merge graph
needs it (one more column on the same row select — no join, and the row
`SELECT` stays named-column, never `p.*`), but it is **not** serialized to
the client: run identity decides edges server-side and the client has no use
for it.

**Scope invariant — one rule, four sites.** Merging gives four
different surfaces the chance to cover more than the user was looking
at: the card the grid renders, the detail view that card opens, the
mutation it fires, and — on the status axis — what the card *says*
about its own members. They are one rule, stated once here and applied
below rather than patched per site:

> A card, the detail view it opens, and the mutation it fires are
> defined over **exactly the row set the user could see when they
> clicked** and over **exactly the one card they clicked**, and the
> card is described by **all** of its members — never a superset of the
> first two, never a sample of the last.

Four corollaries, each binding on one site:

1. **The server builds cards over the rows it returns.** The merge
   graph is built over the rows `/api/predictions` actually returns —
   after `collection_id` (which restricts the candidate `photo_ids`,
   `app.py:15306-15311`), after `rules`, and after the visual clause
   that `_apply_visual_to_rules` folds into `rules` before the query
   runs (`app.py:15300-15302`) — never over the whole workspace. A row
   those filters removed is absent from the graph input, so it cannot
   bridge two components, and `card_id` is a truthful card identity
   under the server-applied scope by construction. (Client-applied
   predicates are a different matter — corollary 2.)
2. **The client renders merged cards only while it displays exactly
   that row set.** Any client-side step that drops a returned row sends
   it to per-node cards and `node_id` handles — see "Filter semantics"
   and "Active-filter detection" below.
3. **Every server entry point that re-expands a handle takes the same
   scope and the card's frozen membership, and rebuilds its own row
   set.** That is both `POST /api/predictions/card` ("Card detail
   endpoint" below) and the accept/reject POST (§3, step 1). Neither may
   delegate to a route that cannot enforce the scope, and neither may
   act on a row set that differs *in either direction* from the
   membership the card had when it was rendered — "when they clicked" is
   part of the invariant. A component that grew between the GET and the
   click (taxonomy cache resolution, a new classify run) and a card that
   split under the user are one event as far as the server can tell, and
   both refuse the click outright and say so, rather than absorbing the
   newcomers or resolving whichever members are left. The row set the
   mutation writes is always the server's own recomputation; the client's
   frozen membership is the precondition it is checked against, never the
   selector (§2, "Shrinkage is a stale click, not a smaller click").
4. **A card's status, badge and actions are an aggregate over all its
   members**, never whichever row won the dedup sort — and **an action
   the card offers applies to every one of those members**, whatever
   each member's current status. These are two halves of one clause,
   because "described by all of its members" constrains both what the
   card may claim and what its buttons must do. A single status
   truthfully describes all members only when the members **agree**, so
   the aggregate is unanimity or `mixed`, and `mixed` is never terminal;
   and a button labelled with the card's own name has to leave the card
   in the state it named, so an action that reconciles only *some*
   members is not a card action at all. Spelled out in the first bullet
   under "Client changes" and in §3 step 3.

Corollaries 1-3 are the row axis, corollary 4 is the status axis, and
the card axis is §3's rule that a `node_id` mutation runs no sibling
expansion. Where a passage below appears to disagree with the
invariant, the invariant wins.

Note what corollary 4's second half rules out. An earlier revision
aggregated status correctly but left the accept path flipping only
`pending` members, which is the status-axis version of the same
sampling error: a card holding an accepted and a rejected member is
described by neither word, and a card whose accept leaves a rejected
member behind has told the user "accepted" about rows that are not.
Both are closed below, and they have to be closed together — an
explicit `mixed` state without reconciling actions is a card the user
can see but cannot resolve, and reconciling actions without an explicit
`mixed` state leave the pre-existing mixed rows (§2 "Reachability")
with no defined rendering.

**Client changes (`review.html`).**

- `getVisibleItems` dedups by `card_id` (fallback to `group_id` then
  prediction id for old payload shapes during rollout).
- **Once `getVisibleItems` returns cards, every caller of it must be
  re-read as operating on cards** — the toolbar "Accept All"
  (`acceptAllPending`), its count in `renderButtons`, and the `A`/`S`
  keyboard handler all currently reason in raw pending *rows*. They are
  not incidental: a toolbar that iterates rows accepts part of a merged
  card and leaves the rest, which is the badge-vs-rows contradiction
  this section exists to prevent, reached without ever clicking a card.
  Re-reading them as cards is necessary but not sufficient — `pending`
  rows and *actionable cards* are different sets once `mixed` exists, in
  both directions — so all three bind to the single
  `getActionableCards()` predicate §3 defines rather than to
  `getVisibleItems()` or to a `status === 'pending'` filter.
  §3 "Every mutation entry point, enumerated" lists each one and its
  disposition; Phase 5 converts them all in one go.
- **Card status and actions are aggregated across every member row**, not
  read off whichever row won the dedup sort, and the aggregate is
  **unanimity or `mixed`** — there is no fourth thing a card can be. A
  member row's own status is exactly one of `pending`, `accepted`,
  `rejected` (`alternative` rows are attached to a parent row as
  alternatives and are never card members — "Edge cases";
  `review.html:1491`), so the member-status *set* is one of seven
  non-empty subsets and the aggregate is a total function on them:

  | member statuses | card status | badge | actions |
  | --- | --- | --- | --- |
  | `{pending}` | pending | "Pending" | Accept · Reject |
  | `{accepted}` | accepted | "Accepted as X" (disabled, `review.html:1454`) | undo hook only |
  | `{rejected}` | rejected | "Rejected" (disabled, `review.html:1456`) | none (reject is non-undoable — below) |
  | `{pending, accepted}` | mixed | "Mixed — 1 pending · 1 accepted" | Accept all · Reject all |
  | `{pending, rejected}` | mixed | "Mixed — 1 pending · 1 rejected" | Accept all · Reject all |
  | `{accepted, rejected}` | mixed | "Mixed — 1 accepted · 1 rejected" | Accept all · Reject all |
  | `{pending, accepted, rejected}` | mixed | "Mixed — n pending · n accepted · n rejected" | Accept all · Reject all |

  The three unanimous rows are the states the design already had. The
  four `mixed` rows are the ones an earlier revision left unspecified:
  its rule ("pending if any member is pending, accepted only if all,
  rejected only if all") is a *partial* function — it silently mapped
  `{pending, accepted}` and `{pending, rejected}` onto the plain
  "Pending" badge, and `{accepted, rejected}` onto nothing at all.

  **Why `mixed` is its own state, and not "just call it pending".**
  Mapping `{pending, accepted}` to a bare "Pending" badge gets the
  *actions* right (the card is still actionable) and the *description*
  wrong: the card is telling the user "nothing here has been decided"
  while one of its members has already been auto-accepted and tagged the
  photo. That is the same class of claim as a rollup reporting
  "completed" when one of its items failed — a status that reads as
  clean over members that are not. `CORE_PHILOSOPHY.md` ("Show the user
  what's happening / No black boxes") governs, and the repo convention
  for rollups is that a mixed outcome reports as the *worse* outcome
  rather than glossed as success. Applied here: a card is **resolved
  only if every member is resolved the same way**, and anything else is
  reported as unresolved *and named as mixed*, with the per-status
  member counts in the badge so the breakdown is legible without
  opening the card. The model chips already show one chip per model
  (next bullet); each chip carries its own member's status, so the user
  can see *which* model said what before choosing.

  **The actions are "Accept all" / "Reject all", and they reconcile.**
  On a `mixed` card both actions are rendered and each one sets **every**
  member to the chosen status — including members already terminal in
  the other direction (§3 step 3). The labels say `all` because the
  click is destructive to at least one prior decision, and the badge
  above them states which decisions exist. This is the only exit from
  `mixed`, and it must exist: `prediction_reject` is in `_NON_UNDOABLE`
  (`db.py:18756`), so a rejection cannot be walked back through edit
  history, and a `mixed` card without reconciling actions would be a
  permanent dead end holding two contradictory model decisions — exactly
  the "leave the two model decisions permanently contradictory" outcome
  this rule exists to prevent.

  **Reachability (why the state is real, and cannot be designed away by
  changing the mutation alone).** `mixed` is reachable *before any card
  is ever clicked*, so an explicit rendering is mandatory regardless of
  what accept does: (a) the merge is new, but the rows are not — a
  catalog can already hold an accepted BioCLIP row and a rejected iNat21
  row on the same burst, decided separately when they were separate
  cards, and the first `all`-tab load after this ships merges them;
  (b) Compare's `accept_subject_species` and `prediction_reviewed` path
  resolve individual rows outside Review entirely; (c) a `node_id`
  mutation deliberately touches only its own node (§3 step 3), so
  resolving two visible sibling nodes in opposite directions under a
  filter and then clearing the filter produces a mixed merged card by
  design; and (d) undo restores each row's *prior* status (§3, "Undo
  restores prior status, not `pending`"), and a prior status set can
  itself be mixed. Making card actions total (§3 step 3) removes the one
  path that the *card* itself created; it does not remove (a)-(d).

  **Which tab a `mixed` card appears under.** The status tabs filter
  *rows*, not cards (`review.html:1298-1300`), and a non-`all` tab is a
  client-applied predicate, so it triggers the per-node fallback
  (corollary 2): under the "pending" tab the user sees per-node cards
  built from the pending rows only, and the aggregate is computed over
  exactly those visible rows — honest, because the tab itself names the
  status being shown. `mixed` is therefore an `all`-tab rendering, which
  is precisely the view where the merge happens and where the
  contradiction is visible. "Members", throughout this bullet, means the
  rows the card was built from under the scope invariant — never rows
  the user could not see.

  The aggregate is computed from the full pre-dedup row bucket for the
  `card_id` (or `node_id` under a filter — §2 "Mutation ID from the
  fallback view"), not from the surviving representative. Deriving
  badge/actions from a representative row is the exact motivating bug:
  when BioCLIP-2.5 has already auto-accepted "Blue Tit" on the burst and
  iNat21's "Eurasian Blue Tit" arrives pending on the same photos, the
  merged card's sort-winning row can be the accepted BioCLIP row and the
  card would render as Accepted with no visible action — silently
  collapsing the pending duplicate that the user needs to see and
  resolve. The aggregate rule forces the card to surface the action
  whenever any duplicate survives — as `mixed`, since the members
  disagree — and the accept path (§3) then flips every member to
  accepted in one click. Symmetric for reject. The rule also makes card
  status robust to sort-order changes (confidence order vs.
  capture-time order vs. id order): the aggregate is a set predicate
  over member statuses, so no ordering choice can flip a card between
  "actionable" and "already resolved". *Test fixtures (Phase 3):* a
  **mixed-status card fixture** — one card containing an accepted
  BioCLIP row and a pending iNat21 row on the same photo set renders as
  `mixed` ("Mixed — 1 pending · 1 accepted") with Accept all / Reject
  all visible, under every representative-row sort order tried
  (species-string asc, confidence desc, prediction-id asc); accepting
  resolves both members (§3 sibling pass); the card then renders as
  accepted with no action. A **mixed-terminal card fixture** — an
  accepted BioCLIP row and a **rejected** iNat21 row, no pending member,
  constructed without clicking anything (the pre-existing-rows case):
  the card renders as `mixed` ("Mixed — 1 accepted · 1 rejected") with
  both actions enabled, is *not* rendered as accepted, rejected, or
  pending, and is not reported anywhere as resolved; "Accept all" leaves
  every member accepted and the card unanimous; the symmetric variant
  asserts "Reject all". A **status-totality fixture** enumerates all
  seven member-status sets against the table above and asserts every one
  maps to a defined badge and action set — the guard that keeps the
  aggregate a total function if the status vocabulary ever grows.
- The card shows: union photo count; one chip per model with that model's
  consensus confidence and vote counts (e.g.
  `BioCLIP-2.5 92% · iNat21 88%`); the display name (§4).
- The group review modal opens with the union membership, through the
  scoped card endpoint below.

**Card detail endpoint.** New endpoint `POST /api/predictions/card`,
with a JSON body:

```json
{
  "id": "<card_id>",
  "member_prediction_ids": [1234, 1235, 1236],
  "rules": [],
  "collection_id": null,
  "visual": null
}
```

It returns the union of member groups with per-photo, per-model rows. It
carries the **server-applied** scope and the card's frozen membership,
and it rebuilds the scoped row set itself rather than composing
`/api/predictions/group/<group_id>`.

*The endpoint is a read; the POST is transport, not semantics.* It
writes nothing, is safe to retry, and takes a body only because its
payload has no bound ("Why the membership is not on the URL" below).
Said explicitly so nobody later "corrects" it back to a GET. Losing HTTP
caching costs nothing here: the response depends on the taxonomy cache
and on live row statuses, both of which move under it, so it was never
cacheable.

Both halves are load-bearing, and neither is covered by the per-node
fallback. That fallback triggers on the four *client-applied*
predicates; `rules`, `collection_id` and `visual` are not among them,
because the server enforces all three on the GET rather than the client
hiding rows after the fact. So a Review view with an active collection,
universal-filter rule or visual-search clause — and no client-applied
predicate — is a **merged-card view**: it
dedups by `card_id` and opens this endpoint. An `id`-only URL gives the
endpoint no way to know which scope produced the card, so it would
rebuild the component over the whole workspace and hand back member
groups, rows and photos the Review GET excluded — under an active
collection, photos outside the collection entirely. That is the same
"the card covers more than the user could see" failure the mutation
scope tuple closes on the POST side (§3), left open on the GET side.

Composing `/api/predictions/group/<group_id>` cannot fix it either:
that route is `db.get_group_predictions(group_id)` and nothing else
(`app.py:15830-15837`) — it resolves a group's full membership from the
`group_id` alone, with no scope parameter to enforce and no rules
evaluation, so any scope the card endpoint accepted would be discarded
one layer down. The card endpoint therefore runs the
same resolution `/api/predictions` runs — `collection_id` →
`get_collection_photos` → `photo_ids`, `rules`, and
`_apply_visual_to_rules` — over its own query, then returns the
component containing the anchor within that row set. The group route
stays for its existing non-merged callers; the card endpoint does not
build on it.

The endpoint takes the card's frozen membership in the same body
(`member_prediction_ids` — the identical field name and shape the
mutation POST carries) and compares it against its own recomputed
`server_members` exactly as the mutation does (§3 step 1) — but, being a
read, it *renders* the intersection and discloses the difference rather
than refusing. A detail view is not destructive, but a card that
opens onto a group the user never saw in it — because the taxonomy
cache resolved between the grid render and the click — is the same
"covers more than the user could see" failure one surface earlier, and
the modal is where the user decides whether to accept. Absent
`member_prediction_ids`, the endpoint returns the whole scoped component
(the legacy shape) and flags `"expanded"` so the client can say so.
It reports divergence in the *other* direction too — frozen members the
current component or scope no longer covers — and it does **not**
inherit the mutation's 409 in either direction: the read returns the
survivors with `"departed"`, `departed_prediction_ids`,
`joined_prediction_ids` and `current_cards`, and the modal disables its
action buttons whenever either list is non-empty (they would 409
anyway). §2, "Shrinkage is a stale click, not a smaller click", states
why the read and the write diverge on exactly this point: a read that
shows the wrong rows can be corrected by looking again, a write that
touches them cannot.

**Why the membership is not on the URL.** An earlier revision put it
there (`&rows=<prediction_id>,…`), and that does not survive contact
with a real catalog. Membership is frozen at *row* granularity
("Freezing rows, not nodes", §3 step 1) and is unbounded by
construction: a burst holds one row per photo, the same-taxon-plus-
overlap edge chains bursts transitively, and two classifier models
double the row count over the same photos — so a component spanning a
few thousand photos serializes tens of kilobytes of ids, well past the
~8 KB request-line and header limits Werkzeug and every common reverse
proxy enforce. The failure lands as a 414/400 on exactly the largest
cards, which are the ones most worth opening, and it lands *after* the
card rendered fine — the user sees a card and cannot open it. `rules`
and `visual` are JSON blobs drawing on the same budget, so the URL was
the wrong place for this payload even before membership joined it.

Moving the whole payload into a body removes the bound and makes the
detail read and the mutation POST the *same shape minus the action*.
That symmetry is a correctness property rather than tidiness: corollary
3 and §3 step 1 both require that every re-expanding call carry the
identical scope tuple, and one shared client helper that builds both
payloads leaves one place to forget it instead of two.

*Two alternatives rejected.* A **server-issued snapshot token** — the
Review GET stores each card's membership and returns a short handle the
client echoes back — keeps the freeze guarantee intact, but the GET
builds *every* card on the page, so it would persist a membership record
per card per refresh (hundreds to thousands per load, on the page the
user reloads most) purely so that the occasional modal open can name
one. It also adds a TTL, a GC path, and a new "snapshot expired" failure
on a click that previously always worked. Server state buys nothing here
that a request body does not. A **membership digest** (count plus hash,
which does fit in a URL) cannot do the job at all: the rule is that
growth discovered at re-expansion is *excluded*, not merely detected
(§3 step 1), and an intersection cannot be computed from a hash.

`visual` is **not** optional in this payload, and it is not
belt-and-braces.
A visual clause is server-applied like `rules` and `collection_id`
(`_apply_visual_to_rules` folds it into `rules` before the query,
`app.py:15300-15315`; the client sends the clause and does not remove
any row afterwards), so it does not trip the client-side fallback and a
Review view whose only active filter is a visual clause reaches this
endpoint by design. Omitting `visual` here would rebuild the component
over the unfiltered workspace and hand back exactly the rows the visual
clause excluded — the same failure as omitting `collection_id`.

**Card ID encoding.** `card_id` is treated as opaque bytes on the wire.
Node keys carry model and fingerprint strings *and* the folded
`species_key` ("Node identity" above), so for `name:`-keyed cards the
folded label is literally inside the encoded id, and those fields come from arbitrary
user-supplied inputs that may contain `/`, `?`, `#`, `%`, or other
URL-significant characters — and may contain the delimiter characters
(`|`, `:`) that appear inside taxon keys and node keys themselves.
Two-part rule:

1. The id never travels in a URL path segment or query string on the
   request path: both callers — the card detail read and the mutation —
   carry it as a JSON field in the request body ("Card detail endpoint"
   above). That retires the Flask routing hazard outright instead of
   working around it: a `<card_id>` path converter does not match a
   decoded slash even when the client uses `encodeURIComponent`, so a
   path-segment id for a label like `hawk/owl` would 404. The id still
   appears in places that are not request bodies — the URL hash for deep
   links, DOM attributes, log lines — which is what rule 2 exists for.
2. The server-emitted `card_id` string is base64url-encoded (RFC 4648
   §5, unpadded — alphabet `[A-Za-z0-9_-]`) over a **structured**
   payload, not a delimiter-joined string. Concretely, the payload is
   the UTF-8 encoding of `json.dumps([smallest_member_key],
   separators=(",", ":"), ensure_ascii=False)` — a single-element JSON
   array wrapping the anchor node key. JSON string escaping makes any
   byte inside the anchor unambiguous — including `|`, `:`, `"`, `\`,
   `/`, and control chars — so the server can decode with `json.loads`
   and recover exactly `smallest_member_key` regardless of what a
   classifier model name looks like. Base64url over the JSON keeps
   the id safe to embed anywhere (DOM attributes, path segments if
   some future route wants them, log lines) without further escaping,
   and keeps it opaque to the client. Where the client persists an id
   (e.g. in URL hash for deep links), it stores the already-encoded
   form verbatim. *Alternative implementation, same guarantee:* an
   opaque digest (e.g. SHA-256 of the canonical JSON) with a
   server-side lookup table from digest → `member_key`; equivalent
   correctness, one extra table lookup per card open. Rejected as
   unnecessary — the structured base64url form is round-trip decodable
   without state.

*Why the anchor alone, and not `(taxon_key, anchor)`.* An earlier draft
prefixed the id with the card's `taxon_key` "so distinct taxa cannot
collide". That prefix is redundant, because a node belongs to at most
one component per Review payload: the merge graph builds components on
`(same taxon_key, overlapping photos)` and a node has exactly one
`taxon_key` at a given time — by §1's per-label resolution, which returns
one key for every row sharing a label and so cannot hand two keys to one
node — so no two distinct-taxon components can share the same anchor
node. Worse, embedding `taxon_key` made the id
brittle across taxonomy-cache transitions: §1's background resolver
opportunistically enqueues any `name:`-fallback label the GET emits,
and the resolver can persist a hit *between* the GET that stamped a
`name:`-keyed `card_id` and the POST that submits it. Rebuilding the
graph from stored rows then produces a `taxon:`-keyed `card_id` for
those same rows and the client's submitted id decodes to a `taxon_key`
that no current component carries — so a card still on screen would
return 400 or, worse, resolve to nothing at all. Encoding only the
anchor eliminates the dependency: the anchor node's stored rows are
untouched by the cache transition, `smallest_member_key` decodes to
the same tuple, and "Anchor lookup and cache-transition safety" below
covers how the server rebuilds the card under the *current* taxon
key.

**Anchor lookup and cache-transition safety.** On a `card_id` mutation
POST the server:

1. Decodes `card_id` to recover `smallest_member_key`.
2. Locates that node's stored rows (join `prediction_review` with
   `predictions` under the active workspace, filtered by the node's
   `(classifier_model, labels_fingerprint, group_id, species_key)`
   tuple for grouped rows, or `prediction_id` for singletons). A node
   whose rows have all been deleted or a bucket a re-run rewrote
   returns 400 — the same stale-handle response the design already
   specifies elsewhere.
3. Rebuilds the scoped candidate row set (the scope tuple as §3
   specifies), then computes taxon keys over it *now* using §1's
   per-label resolution — `S(L)` gathered from that same row set, the
   current cache read once, a hit that landed between the GET and the
   POST resolving to `taxon:...` and a miss staying `name:...`. The
   order matters and is load-bearing: canonicalization is a property of
   a row *set*, so the set has to exist before any key does. Computing a
   key from the anchor node's rows alone would reintroduce exactly the
   per-row divergence §1 removes, one scope narrower.
4. Runs the same card-building graph the GET runs over those rows and
   returns the connected component that contains the anchor node. GET
   and POST therefore canonicalize over the same scoped set and cannot
   disagree about a key except through a cache transition, which step 5
   bounds.
5. **Checks the frozen membership the POST carried
   (`member_prediction_ids`, §3 step 1) against that component**, and
   mutates the component — not the client's list. `server_members =
   resolved component ∩ scope` is the row set written, and the sibling
   scan is bounded by the same set (§3 step 3). The component is how the
   server *finds* the card, enumerates each member's rows, and defines
   the write; the frozen membership is the client's claim about what the
   card *was* when the user clicked, and its only job is to license or
   refuse the write. The two must be **equal as sets**: a frozen row that
   landed elsewhere or that the re-applied scope now excludes, and
   equally a row the server finds that the client did not name, both mean
   the card the user clicked is no longer the card the server would act
   on, and step 5 returns 409 `card_changed` instead of writing a
   partial. Checking the client's list against a set the client did not
   help build is the whole point — see "Shrinkage is a stale click, not a
   smaller click" below, and "The precondition is verified against a set
   the client did not help build" within it.

The intended cache-transition sequence is: GET emits a `name:blue tit`
card with anchor `A`, `card_id` = base64url(JSON([`A`])), and
`member_prediction_ids` = the rows that card displayed. Background resolver
populates the cache for "blue tit" → *Cyanistes caeruleus* (iNat
13094). User clicks Accept. POST sends `card_id` plus that membership.
Server decodes to `A`, computes the taxon key from `A`'s rows now —
`taxon:13094` — builds the graph and finds `A`'s component. No 400 and
no lost click: that is what recomputing the key buys, and it is the
whole reason `taxon_key` is not baked into `card_id`.

What the transition must **not** do is grow the click. The new key can
pull in a group `B` from another model that already resolved to
`taxon:13094` and therefore rendered as its own separate card at GET
time. `B`'s rows land in `server_members` but not in
`member_prediction_ids`, so the two sets disagree and the POST is
**refused with 409 `card_changed`**, naming `B`'s rows in
`joined_prediction_ids`; nothing is written, `B` stays pending, and the
next Review load draws the merged card for real. An earlier revision
instead excluded `B` and let the click proceed with `"expanded": 1`.
That is no longer available, and the reason is not `B` — it is that
"the client's list is short because the card grew" and "the client's
list is short because the client dropped a member" are the same
observation to the server ("The precondition is verified against a set
the client did not help build", below). Reapplying the filter scope does
not substitute for the membership check — `B` satisfies the same scope —
and neither does any property of the anchor: the anchor is an identity,
not a membership. Only the comparison of the frozen membership against
the server's own recomputation carries "is this still the card the user
saw".

**Shrinkage is a stale click, not a smaller click.** Step 5's check
splits the shrink direction on a bright line: is every frozen row still
in the anchor's component?

- **Yes, only the anchor moved.** The transition changed which member
  is the smallest, but every frozen row still resolves into the same
  component. `server_members` and the frozen membership are equal, the
  mutation applies to the whole card, and the click resolves the card
  the user clicked. This sub-case needs nothing extra.
- **No, the component split.** A new classify run, or any other
  request-scope input that flips §1's per-label resolution for one
  label but not another, can move part of the card into a different
  component. Concretely: a new prediction row introduces a conflicting
  `scientific_name` for one of the card's labels L1, which §1's
  request-wide conflict rule (see §1, "Conflicts") resolves by keying
  L1 as `name:blue tit` for the whole request, while an alternate
  label L2 that only some members carry still resolves to `taxon:X`.
  The anchor rebuilds under L2 into a smaller `taxon:X` component; the
  L1-only frozen members end up in a `name:` component the POST does
  not reach. Silently intersecting the frozen membership with the
  smaller component would leave those members pending — the same
  badge-disagrees-with-metadata failure the retraction rule closes
  ("A card mutation writes every member status before it decides any
  keyword effect" below), arriving through a different route. "Accept
  all" cannot mean "accept the survivors of a card the user never saw".

Step 5's check is exactly this bright line, and it is stated over the
*mutated* set rather than the component alone, because a frozen row can
leave by either route:

> **Completeness precondition.** `member_prediction_ids` must equal
> `resolved component ∩ scope` — as sets, in both directions.

The component half catches the split above. The scope half catches the
other way a frozen member departs, which has nothing to do with
taxonomy: a member whose status changed under an active status tab, or
whose photo left the selected collection, between the GET and the POST.
`∩ scope` drops it just as quietly, and the user's "Accept all" would
narrow just as invisibly. From the user's side the two are one event —
the card they clicked is no longer the card the server can act on — so
they get one rule and one response.

**The precondition is verified against a set the client did not help
build.** An earlier revision stated this check as *"the mutation runs
only when `resolved component ∩ member_prediction_ids ∩ scope` equals
`member_prediction_ids` in full"* — and that is not a precondition at
all, because both sides of it are downstream of the same client input.
Drop a displayed member from the payload and the intersection shrinks to
match, the equality holds, and the server performs a partial "Accept
all" on a card the user never saw. A condition the caller can satisfy by
lying is decoration. What follows is the correction; it is a change of
*which set is authoritative*, not an extra guard bolted on top.

*What the server knows by itself.* `card_id` is not a client-minted
token the server takes on trust: it decodes to an anchor node key, and
step 1 of §3 then rebuilds everything else from stored state plus the
scope tuple — the scoped row set the GET saw, current taxon keys under
§1's per-label resolution, the card graph, and the component containing
the anchor. Name that set

```text
server_members = resolved component ∩ scope
```

(for a `node_id` request, the named node's own rows ∩ scope). Every term
in it is server-derived. `member_prediction_ids` appears nowhere in its
construction, which is precisely what makes it usable as the thing the
client's claim is checked against.

*The inversion.* `server_members` **is** the mutation boundary — the row
set that gets written. `member_prediction_ids` is **not a row selector**;
it is an If-Match assertion about what the client rendered, and its only
effect is to permit or refuse the mutation. Verified by set equality
against `server_members`:

| Client's list vs. `server_members` | Meaning | Result |
| --- | --- | --- |
| equal | the card is still the card | mutate `server_members` |
| client ⊊ server | rows joined the component or the client under-reported — **the server cannot tell which** | 409, `joined_prediction_ids` |
| client ⊋ server | the card split, or a member left the scope | 409, `departed_prediction_ids` |
| neither | both at once | 409, both lists |

**What is frozen is the card's rendered state, not just its
membership.** The equality above compares *ids*, and a card can change
under the user without any id moving. `server_members` is
`resolved component ∩ scope`, and on the unfiltered `all` tab `status`
is not one of the scope predicates at all — so a member that Compare
accepted while the Review grid sat open leaves the component, the scope
and therefore both sides of the check identical. The `∩ scope` half
catches status drift only when it crosses an *active* status tab, which
is the one case where the user had a filter on; the ordinary case
passes.

What passes with it is the failure this section exists to forbid,
arriving on the status axis instead of the membership axis: the user
reads a `pending` card, clicks its ordinary **Reject**, and the click
silently reverses an acceptance they never saw — flipping that member
out of `accepted`, retracting its keyword through the reconciling-reject
rule (§3, Phase B), and recording the flip as a **non-undoable**
`prediction_reject` (`_NON_UNDOABLE`, `db.py:18756`). The card never
rendered `mixed`, never labelled its buttons "Accept all" / "Reject
all", and never disclosed a destructive click. Corollary 4 and the
`mixed` badge exist precisely so a reconciling click is stated before it
happens; a stale render routes around both.

So the frozen object is the pair. The mutation POST carries
**`observed`** — a map from prediction id to the status the client
rendered for that row — and `member_prediction_ids` **is its key set**,
not a second field alongside it: one field carries both halves, so they
cannot disagree. The name and shape are #1489's, which already sends
`observed` from the burst modal for the same reason. The precondition is
then one comparison against the server's own recomputation:

```text
observed == { p: COALESCE(pr_rev.status, 'pending') for p in server_members }
```

as maps. The key-set half is the completeness precondition above,
unchanged, with its `departed`/`joined` lists; the value half adds
`changed_prediction_ids`, each entry naming `from` and `to`. Both halves
land on the same 409 `card_changed`, because from the user's side they
are one event — the card they clicked is not the card the server can act
on. The card *detail* endpoint takes no baseline and is unaffected: it
renders current statuses and refuses nothing, and the mutation its modal
then issues carries the statuses **that modal** rendered.

*Compare-and-swap, not "still undecided".* The precondition is that each
member is what the client displayed, **not** that it is pending.
Refusing decided members would break corollary 4 outright: "Reject all"
on an `{accepted, pending}` card is a deliberate re-decision of an
`accepted` member, and it is the only route Review offers for revising
an accept-vs-reject contradiction. #1489 drew exactly this distinction
for the burst modal — a re-decision the client saw passes, while "a
decision that landed from Browse or a second tab after the modal opened
does not" — and this is that rule at card granularity.

*Why 409 rather than #1489's skip-and-report.* #1489 skips the moved
unit and applies the rest; it also chose its unit deliberately,
escalating from the row to the **photo** because
`update_predictions_status_by_photo` "restates every prediction of the
photo, so one stale member invalidates the whole photo's write, not just
its own row". The rule to carry across is that one, not the verb: **the
precondition's granularity is the write's granularity.** A card
mutation's write unit is the whole card — Phase A writes every member's
status and Phase B then derives *one* keyword effect from all of them
(§3) — so skipping a moved member would leave the card partially
resolved *and* compute the keyword decision over a membership the user
never saw. One stale member invalidates the card's write for the same
reason one stale member invalidated #1489's photo. This is also the
membership rule's own answer, one axis over: a row is its own decision,
a card is one claim.

*Two narrower repairs, both rejected.* Comparing the card's **folded
badge** rather than the member vector is lossy in the place it matters:
the badge is a total function of the member statuses, so
`{accepted, pending, rejected}` and `{accepted, rejected, rejected}`
both render `mixed`, while *which* members are accepted decides which
keywords Phase B retracts and what the prior-status snapshot restores on
undo. And exempting drift that agrees with the requested outcome — the
member is already `accepted` and the click is "Accept all" — makes the
precondition action-dependent, and is wrong even on its own terms: the
history entry discloses what the click overrode ("Accepted 3 predictions
(1 previously rejected)") and undo restores each row's prior status, so
a status the client never saw changes both. Strict equality, no
exemptions.

*Where it is read.* #1489 puts every prediction-decision route under a
writer lock taken **before its first precondition read**
(`_begin_prediction_decision`; the route list `_PREDICTION_DECISION_ROUTES`
is checked against `create_app`'s own call graph by a route-contract
test, so a new deciding route fails until it is listed and locked). The
card mutation is such a route and joins that list, which is what makes
the compare-and-swap a compare-and-swap: the membership rebuild, the
status comparison, Phase A and Phase B all sit in one serialized
section, and no decision can land between the check and the write.

*Why equality and not a subset check in the safe direction.* The earlier
revision argued the two directions were asymmetric — "growth's extra
rows were never part of the click, so exclude them and report
`expanded`; shrinkage's missing rows *were*, so refuse". That asymmetry
is real in the user's intent and invisible from the server's position:
growth and under-reporting present as the identical relation
`member_prediction_ids ⊊ server_members`, and separating them would
require knowing which cards the *previous* taxonomy cache state would
have drawn — state the server does not keep. So the two collapse into
one rule, and the cost is bounded and stated: the rare cache-transition
merge now refuses the click instead of accepting the clicked half and
reporting `"expanded": N`. That is the same trade this section already
made for the split direction, applied in the direction it was skipped.

*What tampering can still buy, stated honestly.* A forged payload that
happens to equal `server_members` passes. That is not an escalation: the
mutation writes `server_members` either way, so the most a forgery buys
is *suppressing the 409* an honest client would have received —
behaving as though the user had reloaded and clicked. It can never reach
a row outside `component ∩ scope`, because it names no rows at all. The
earlier text claimed "the intersection is what makes a tampered
membership harmless"; that was right about widening and silent about
narrowing, which is the half that mattered.

*Why not a signed digest or a persisted snapshot.* Both were considered
and both are redundant here. An HMAC over the GET's membership
authenticates *provenance*, not *freshness* — a correctly signed digest
from a GET thirty seconds ago is still a valid statement about a card
that has since split, so the server must recompute and compare anyway;
and once it recomputes, the signature adds nothing, because any list
that matches the recomputation is by construction the server's own set.
A server-side snapshot per rendered card buys the same answer for a
per-GET write, a TTL and an eviction policy, in a single-user local app
where the "attacker" is the user's own browser. The rebuild the mutation
already performs for anchor resolution *is* the authentication; the bug
was using it to filter the client's list instead of to replace it.

The precondition is evaluated **inside the serialized section
`_begin_prediction_decision` opens** (above) and before any Phase-A
write ("A card mutation writes every member status before it decides
any keyword effect" below). On failure the server writes nothing at all
and returns **409**:

```json
{
  "error": "card_changed",
  "departed_prediction_ids": [1235, 1236],
  "joined_prediction_ids": [],
  "changed_prediction_ids": {"1234": {"from": "pending", "to": "accepted"}},
  "member_count": 8,
  "departed_count": 2,
  "joined_count": 0,
  "changed_count": 1,
  "current_cards": [
    {"card_id": "…", "prediction_ids": [1234, 1237]},
    {"card_id": "…", "prediction_ids": [1235, 1236]}
  ]
}
```

All three are always present, and all three are computed against
`server_members`: `departed` is `member_prediction_ids \ server_members`,
`joined` is `server_members \ member_prediction_ids`, and `changed`
covers the members present in both whose current status is not the one
`observed` recorded. The client's inline notice reads from whichever is
non-empty ("this card split — 2 predictions moved" / "2 more predictions
joined this card" / "1 of its 8 predictions was decided elsewhere while
this card was open"); the recovery is the same reload either way.

**409, not the 400 the anchor-gone case uses**, and the distinction is
load-bearing rather than pedantic. 400 means the handle names rows the
server cannot resolve: the click is unrecoverable and a reload is the
only move. 409 means the handle resolved perfectly and the *card*
moved: the rows are all still there, the body can name the current
decomposition, and the client has something specific to say. Reusing
the 400 shape would force the client to distinguish the two by
inspecting the body anyway, and would make "the anchor is gone" and
"your card split in two" indistinguishable in logs and in fixtures.
Both divergence directions land on this same 409, for the reason given
under "The precondition is verified against a set the client did not
help build": the server cannot separate growth from an under-reporting
client, so it does not try.

**Why refuse rather than act on the survivors and report the losses.**
The nearer repo precedent points the other way and is worth stating
before departing from it: PR #1489 chose skip-and-report for a batch of
per-row accepts, naming the count it would really act on rather than
failing the batch, so that one moved row could not strand thirty-four
honest ones. That is the right call there and the wrong one here,
because the unit differs. #1489's unit is a row, and each surviving
row's accept is, on its own, exactly the action the user asked for. A
card is not a bag of independent rows — it is a single claim ("these
members all assert *Cyanistes caeruleus* over these photos") and the
click is a single decision about that claim. When members depart, the
claim itself has changed; in the conflict case the server has *just
decided it can no longer assert that taxon for one of the labels*. So
accepting the survivors is not a smaller version of the user's
decision, it is a different decision they were never shown — and this
design has already ruled, four times over (corollary 4, the `mixed`
badge, the toolbar's card-count label, the reconciling reject), that a
card action resolving only some members is not a card action at all.
Retrying is cheap here in a way it is not in #1489: one reload and one
click on a card that now says what it is, against re-deriving which
thirty-four of thirty-eight rows already landed.

**What the user sees.** Silently narrowing is the failure; silently
*refusing* is the same failure wearing the other hat, so the refusal is
never the whole answer. The card is left exactly as it was — still
pending or still `mixed`, both actions still enabled, nothing marked
resolved, no keyword written — and gains an inline notice built from
the 409 body: *"This card changed while it was open — 2 of its 8
predictions now belong to a different species card. Nothing was
changed."* with a **Reload Review** action beside it. Naming the counts
is the point; "something changed, please try again" is `CORE_PHILOSOPHY.md`'s
black box in politer wording. After the reload the rows sit on the two
cards `current_cards` named, each saying what it now claims, and each
can be accepted for that.

**The batch does not abort.** Toolbar "Accept All" issues one card
mutation per displayed card (§3, "Every mutation entry point,
enumerated"), so a 409 is *that card's* outcome and not the run's: the
remaining cards are still submitted. This is where #1489's
skip-and-report shape is right, because at the rollup level the items
genuinely are independent, and failing the run would strand the honest
cards for the sake of one stale one. The rollup then reports under the
repo convention that a mixed outcome takes the **worse** outcome:
"Accepted 12 of 13 cards — 1 card changed and was skipped; nothing was
changed on it. Reload Review." It is not reported as a completed
"Accept All", and each skipped card keeps its own inline notice and its
own enabled actions. Rejecting at the card level and skipping at the
rollup level is one rule applied at two granularities, not two rules: a
card is the unit of the promise, a run is a bag of independent
promises.

**The read discloses; only the write refuses.** `POST
/api/predictions/card` compares against the frozen membership exactly as
the mutation does ("Card detail endpoint", §2) and so observes the same
divergence, in either direction — but it must **not** inherit the 409. A
detail view that refuses leaves the user staring at nothing while the
grid still shows the card, and a read protects nobody by declining to
render. It returns the intersection of the two — the members the user
saw that are still there — plus `"departed": <n>`,
`departed_prediction_ids`, `"expanded": <n>`, `joined_prediction_ids`
and the same `current_cards` decomposition, and the modal renders the
card-changed notice with its Accept / Reject buttons **disabled** —
they would 409 anyway, and a button that cannot fire is its own small
black box. The read may show a subset because a read that shows too
little is corrected by looking again; the write may not act on one
because a write that acts on too little is not correctable at all.

The one shrink this cannot even reach step 5 through is when the
anchor node's rows are gone entirely (bucket rewrite, deletion) —
that returns 400 at step 2 as before. Cache resolution does not
delete rows; it just changes what taxon they resolve to, and the
`name:`↔`taxon:` transition surfaces at step 5 as either a subset
success (the whole card moved together) or the split-and-refuse case
above (only some members moved).

**Filter semantics.** No filter may ever widen a card, but the two
kinds of filter achieve that differently, and conflating them is what
leaves the card endpoint unscoped:

- **Server-applied** — `rules`, `collection_id`, and the `visual`
  clause. All three reach `db.get_predictions` as `rules`: the visual
  clause is resolved by `_apply_visual_to_rules` into an inlined
  `photo_ids` rule and appended to `rules` *before* the query
  (`app.py:5729-5743`, called at `app.py:15300-15315`), and the client
  merely sends the clause as its own `visual=<json>` parameter
  (`review.html:1091-1096`) — it removes no row from the response
  afterwards. These shrink the
  *graph input* (corollary 1): the excluded rows never reach card
  building, so `card_id` stays a truthful identity and merging still
  happens correctly *within* the filtered row set. They do **not**
  trigger the per-node fallback — a collection-scoped or
  visually-scoped Review is still a merged-card view. What they require
  instead is that every server
  entry point which re-expands a handle receives them: the mutation
  POST (§3, step 1) and the card detail call ("Card detail endpoint"
  above), or the re-expansion silently rebuilds a wider component than
  the grid showed.
- **Client-applied** — the four predicates listed below, which hide
  rows *after* the server returned them. The server cannot fold these
  into the graph input, so the client stops trusting `card_id` and
  builds cards from the matching rows only, per node identity. Merging
  becomes an intra-filter no-op and the page shows exactly what the
  visible rows said.

The visual clause belongs on the first side, not the second, and the
distinction is not cosmetic: the two sides get opposite treatment. A
server-applied clause never suppresses merging but must travel on every
re-expanding call; a client-applied one suppresses merging and makes
`card_id` unusable as a handle. Classifying `visual` as client-applied
would demand a fallback the structural detector below cannot even
trigger (the returned row set and the rendered row set are identical
under a visual clause, so `predictions.length !== allPredictions.length`
is false), leaving the specification asking for a card mode no
implementation could produce.

The client-side half keeps every filter honest (a merged card has no
single model, no single fingerprint, no single confidence, and no
single status) and costs nothing: the client groups the visible rows by
the node identity the server stamped on each row. The moment such a
predicate — a confidence slider, a status tab —
removes a row that was a *bridge* between two components, the server's
`card_id` describes a card the user is not looking at. See "Fallback
dedup key" below for what the client uses instead.

*Active-filter detection.* The trigger has **two arms and either one
alone fires it**: (a) any of the four client-applied predicates
enumerated below is truthy against its no-filter sentinel, **or**
(b) the row set the client is about to render is not exactly the row
set the server returned — in today's code,
`predictions.length !== allPredictions.length` after the collection
re-intersection at `review.html:1126-1131`, or `getVisibleItems`
dropping any row before its dedup pass. Neither arm subsumes the
other and both are needed. Arm (b), the row-set arm, is closed under
future predicates — a new filter added to `getVisibleItems` cannot
silently escape it, which is the closure enumerating predicates has
already failed twice here (once by listing only model and
fingerprint, and once by omitting that collection re-intersection,
which drops rows the server returned whenever the cached
`collectionPhotoIds` set is narrower than the collection the server
queried, a stale set after a collection edit). Arm (a), the
enumerated arm, is what catches **active-but-quiet** predicates that
arm (b) cannot see: `currentModel === 'BioCLIP-2.5'` when the return
happens to contain only BioCLIP rows, or `minConfidence > 0` when
every returned row already exceeds it — the raw predicate is
non-default, the client is filtering, but the before/after row set is
identical. Under arm (b) alone the client would keep rendering a
merged card and (as §"Mutation ID from the fallback view" below
spells out, and the reviewer flagged) the mutation contract would
then have no valid shape for it. Arm (a) closes that hole by keying
the render on the raw predicate values, not on the observed drop.

The predicates that cause a divergence today are the four
`getVisibleItems` applies, matching `review.html:1281-1300` verbatim:

- `minConfidence > 0` — hides rows whose `confidence` is below the
  slider value.
- `currentModel && currentModel !== 'all'` — hides rows from other
  classifier models.
- `currentLabelsFingerprint` truthy (non-null, non-empty string) —
  hides rows written under other label-set fingerprints.
- `currentTab && currentTab !== 'all'` — hides rows whose `status`
  differs from the selected tab (`pending` / `accepted` / `rejected`).

`VireoFilter.getVisual()` is deliberately **not** on this list.
`getVisibleItems` does not consult it; the rows a visual clause excludes
were already dropped by `db.get_predictions` and never reached the
client. It is a server-applied filter and is handled as one, above.

Whenever either arm fires — any single one of the four enumerated
predicates being truthy, or any client-side drop the structural test
detects — the client renders the per-node fallback for card
construction. This is stricter than "model or fingerprint only" for
good reason: a below-`minConfidence` bridge row or an already-accepted
`status != currentTab` sibling row is exactly the kind of hidden
bridge that lets the server-computed full-component `card_id` stitch
two visible groups into one displayed card — and lets
the card detail call (`POST /api/predictions/card`) re-expose the
hidden bridge rows on open. Forwarding the same predicates into the mutation POST (§3)
is necessary but not sufficient: without also rebuilding the displayed
card on the client, the *display* would still show a merged card whose
members the mutation then refuses to touch, and the user would see a
click that "does nothing" to visible siblings while silently mutating
hidden ones. So the fallback trigger and the mutation scope tuple stay
symmetric on the client-applied predicates, always. The server-applied
three (`rules`, `collection_id`, `visual`) are symmetric in the other
direction: they never suppress merging, and they travel on *every*
server call that re-expands a handle — the mutation POST and the card
detail call alike, in the same payload field on both.

No sentinel change is required — the existing `currentModel` default
(`'all'`), `currentLabelsFingerprint` default (`null`), `minConfidence`
default (`0`) and `currentTab` default (`'all'`) all resolve to
"no filter active" under the checks above. (If the client is later
refactored to use `'all'` as the labels-fingerprint no-filter sentinel
too, the fingerprint check collapses to `!== 'all'`; the design does
not require that change.)

*Fallback dedup key.* The server computes `card_id` per row over the row
set it returns — the server-applied scope already folded in (corollary
1) — and the client, whenever a client-applied predicate fires, ignores
`card_id`
entirely — both for deduping the displayed row set and for the
subsequent mutation — and instead uses the row's **node identity** — the
same tuple the server uses when building the merge graph (§2, "Node
identity"): `(classifier_model, labels_fingerprint, group_id,
species_key)` for grouped rows, and `(classifier_model,
labels_fingerprint, "p" + prediction_id)` for singletons. The client
does not assemble that tuple itself — the server stamps the encoded
`node_id` on every returned row (§2, "Filter-invariant by
construction"), and the client echoes it back verbatim. Because all
four fields are intrinsic row columns, the `node_id` stamped on a row
is the same value no matter which filters were active on the GET —
which is exactly why the fallback can use it as a mutation handle at
all. Neither a positional subset index nor any computed partition
could be used here: both are functions of which rows the query
returned, so a filter that hid an entire earlier subset would make the
server renumber or re-derive the survivor on the mutation rebuild, and
a valid click would 400. Using node identity — not
`(taxon_key, group_id)` — is essential: (a) singleton rows all carry
`group_id = NULL`, so `(taxon_key, group_id)` would collapse every
ungrouped prediction of the same taxon on unrelated photos into one
displayed row; (b) node identity is exactly the granularity the server's
component graph is nodes-on, and the granularity the merged-card
endpoint does *not* expose across filter boundaries, so the fallback is a strict
subset of what the unfiltered view would show. The merged-card endpoint
is never invoked from a client-filtered view; when it *is* invoked — a
view whose only active filters are server-applied — it carries `rules`,
`collection_id` and `visual` so its union is rebuilt over exactly the
rows the grid was built from (corollary 3, "Card detail endpoint").

*Mutation ID from the fallback view.* Every row the server returns
still carries the full-component `card_id`. In a filtered view that
`card_id` is not a usable mutation handle: when the server-computed
component contains N > 1 nodes and the filter causes the client to
render some subset of them as separate fallback cards, every rendered
row *shares the same* `card_id` (the anchor of the full component), so
a POST that names only `card_id` cannot tell the server which of the
visible fallback nodes was clicked; further, when the filter hides the
node the server anchored on, the `card_id` decodes to a
`smallest_member_key` that isn't in the visible set at all. Both
failures produce silent mismatches between what the user clicked and
what the mutation resolves. So from a filtered view the client sends
the clicked row's node identity tuple verbatim
(`(classifier_model, labels_fingerprint, group_id, species_key)` for
grouped rows, `(classifier_model, labels_fingerprint, "p" +
prediction_id)` for singletons — encoded as the `node_id` the server
stamped on that row) plus the full seven-entry scope tuple from
§3 — the four client-applied predicates and the three server-applied
ones — and does *not* send the server's `card_id`. The mutation POST
therefore distinguishes two request shapes, keyed on the **render
decision** the client already made under "Active-filter detection"
above, not on the raw predicate values: (i) **merged-card view**
(the client rendered a merged card — neither arm of the fallback
trigger fired, which in particular means every client-applied
predicate was at its no-filter sentinel, since a non-default
sentinel — active-but-quiet or not — trips arm (a) on its own and
routes render and mutation to (ii)) — carries `card_id` with the
seven-entry scope tuple populated as the GET sent it, and the server
resolves the full component *within that scoped row set* (which under
this shape can only ever be the three server-applied entries narrowing
it, since the client-applied entries are all at their no-filter
sentinels by construction of the trigger); (ii)
**fallback view** (the client rendered per-node cards — at least one
arm of the trigger fired) — carries `node_id` (the encoded node
identity tuple, same base64url-of-JSON encoding as `card_id` for
uniformity, decoding to `[classifier_model, labels_fingerprint,
group_id, species_key]` for grouped rows or
`[classifier_model, labels_fingerprint, "p" + prediction_id]` for
singletons) plus the scope tuple, and the server treats the card as
exactly that single node, resolving photos only from that node's
members and mutating strictly the node's own rows (§3 "`node_id`
request" — no cross-model taxon-matched sibling scan, so the mutation
cannot reach any other visible node's rows on a shared photo).
Keying the shape on the render decision — instead of on "any
client-applied predicate active," as an earlier revision had it —
closes the mismatch the reviewer flagged: an active-but-quiet
predicate (e.g. a `currentModel` set to the only returned model, or
a `minConfidence` every row already exceeds) leaves the row set
unchanged but is still active, and under a predicate-value-keyed
contract the render would fall through arm (b)'s test and produce a
merged card that the mutation contract had no valid shape for.
Under the render-keyed contract, both arms of the trigger route the
same way: the render is merged and the mutation is `card_id` (the
enumerated arm did not fire because every enumerated predicate was
at its no-filter sentinel, and the row-set arm did not fire because
nothing was dropped), or the render is per-node and the mutation is
`node_id` (either arm fired). There is no third state.
The server resolves a `node_id` by matching those columns on stored
rows and only then intersects the node's members with the scope tuple.
Because node identity is intrinsic to the rows (§2, "Node identity is
a pure function of immutable row columns"), that match is independent
of both the filter state and any concurrent write to `photos`, so a
handle minted on the GET always names the same node on the POST that
follows it. The one way a `node_id` stops resolving is that its rows
are gone — a re-run rewrote the bucket's `group_id`s, or the rows were
deleted — which is a true stale handle, not a boundary artifact.
An unrecognized `node_id` (stale after a re-run) is a 400; a POST
that carries both `card_id` and `node_id` is a client bug and
rejected as a 400. From a merged-card view — including one scoped only
by server-applied `rules`/`collection_id`/`visual` — the mutation shape
is unchanged from what §3 already specifies.

*Why the fallback matters for privacy.* If server-computed components
stitched groups A and C together only through a bridge row B that the
current filter hides — a group at another fingerprint, a
below-`minConfidence` row, a `status != currentTab` sibling, or a
row from another classifier model — opening the "A+C card" from the
filtered view would otherwise re-expose B's hidden rows through the
`POST /api/predictions/card` union. Falling back to per-node
cards *and* per-node `node_id` mutation handles in filtered views
eliminates that exposure entirely: the union endpoint is never called,
and every mutation names exactly one visible node under the same scope
tuple. When the user clears every filter, the full server components
(and the merged card endpoint) apply again.

### 3. Cross-model accept and reject

Generalize the pattern `accept_subject_species` already implements, from
string matching to taxon matching, and from Compare-only to Review.

**Accept.** `db.accept_prediction` currently accepts the clicked prediction
and, if grouped, its group siblings — restricted to `pr.classifier_model =
?`. The taxon-keyed accept operates on the **entire card component's photo
union**, not just the clicked group's photos:

1. **Resolve the card, with the same filter scope the client rendered.**
   The mutation POST carries **either** `card_id` (merged-card view —
   accompanied by `member_prediction_ids`, the card's frozen
   membership, below) **or**
   `node_id` (client-filtered view — see §2 "Mutation ID from the
   fallback view"), never both, **plus every filter that shaped the
   card** — the server-applied ones that shaped the GET's row set and
   the client-applied ones `getVisibleItems` then applied
   on the way to what the card actually
   shows. Concretely the scope tuple is: `rules`, `collection_id`,
   `model` (from `currentModel`), `labels_fingerprint` (from
   `currentLabelsFingerprint`), `min_confidence` (from `minConfidence`,
   `review.html:1281-1283`), `status` (from `currentTab`,
   `review.html:1298-1300`), and `visual` (from
   `VireoFilter.getVisual()`, `review.html:1091-1096`) — server-applied
   filters that affected the GET plus client-applied filters that
   affected the displayed card, one uniform tuple. The client already
   threads `rules`/`collection_id`/`visual` into `/api/predictions`
   (`review.html:1091-1111`) — `visual` as its own JSON-encoded query
   parameter, distinct from `rules`; the mutation is extended to carry
   the full tuple (`review.html:1576-1581` and the reject path)
   verbatim, `visual` included, so the server can call the same
   `_apply_visual_to_rules` (`app.py:5703`) resolver on the mutation path
   and reproduce the exact matched-photo-id set the GET used. Note the
   three hidden failure modes this closes, two client-applied and one
   server-applied: (a) a below-`minConfidence`
   sibling row that bridges two groups would otherwise stitch the
   server's full component through a row the user couldn't see; (b) a
   `status != currentTab` row (e.g. an already-accepted sibling on the
   "pending" tab) could similarly bridge or be re-mutated; (c) a
   same-taxon sibling row on a photo *outside* the active visual
   clause's match set — this one never reached the GET's graph at all,
   because the clause is server-applied, which is exactly why the POST
   must re-apply it: a mutation that carried only `rules` would
   *re-admit* the row on the rebuild and stitch two cards the grid
   showed separately, since `rules` as the client sends it does not
   subsume the visual clause. All three are excluded from the resolved
   component by forwarding the same predicates. For a `card_id`
   request, the server decodes to the anchor node key, rebuilds the same
   scoped row set the GET used, computes *current* taxon keys over that
   set with §1's per-label resolution — the anchor's key falls out of it
   like every other row's; it is never derived from the anchor node's
   rows alone (§2, "Anchor lookup and cache-transition safety") —
   re-runs the same card-building graph under those keys, and returns
   the component containing the anchor. Recomputing the taxon key at mutation time is what
   makes a click safe when the background taxonomy resolver populated
   a hit between the GET and the POST — the anchor is still findable
   and the component still resolves under the new key.

   **The POST also carries the card's frozen state, and the server
   checks it rather than obeying it.** Alongside `card_id`, the client
   sends `observed` — a map from prediction id to the status it rendered,
   covering every prediction row the card it clicked was built from, so
   that `member_prediction_ids` is exactly its key set rather than a
   second field that could disagree with it. It has both halves: the
   server returns each row individually and the client keeps them all
   rather than collapsing them (§2, "Payload changes"), so the displayed
   membership *and* the displayed statuses are literally enumerable
   client-side. The row set that gets **written** is the server's own
   recomputation,

   ```text
   server_members = resolved component ∩ scope
   ```

   which contains no client input beyond the `card_id` handle and the
   scope tuple, and the mutation runs **only if**

   ```text
   observed == { p: COALESCE(pr_rev.status, 'pending') for p in server_members }
   ```

   as maps — the key-set half being `member_prediction_ids ==
   server_members` — otherwise 409 `card_changed`. `observed` is
   therefore an If-Match precondition, not a row selector: it decides
   *whether* the mutation happens and never *which rows* it touches. §2,
   "What is frozen is the card's rendered state, not just its
   membership", works through why the membership half alone lets a click
   silently reverse an acceptance the card never showed, why the answer
   is 409 rather than #1489's row-level skip-and-report, and why the
   comparison is against the rendered status rather than against
   "undecided".

   An earlier revision had this the other way round — the mutated set was
   `resolved component ∩ member_prediction_ids ∩ scope`, gated on that
   intersection equalling `member_prediction_ids` in full. That gate is
   vacuous, because both of its sides are downstream of the client's
   list: omit a displayed member and the intersection shrinks to match,
   the equality holds, and "Accept all" quietly becomes "accept some" on
   a card the user never saw. §2, "The precondition is verified against a
   set the client did not help build", works through the correction, the
   table of the three divergence cases, why the check is now symmetric,
   and why a signed digest or a persisted GET-time snapshot would add
   nothing over the rebuild the mutation already performs. The short
   version: the mutation boundary must be something the server derived,
   and it already derives one.

   **Freezing rows, not nodes.** An earlier revision froze
   `member_node_ids`. Node identity is immutable, but a node's *row set*
   is not fixed relative to a scope: between the GET and the POST a
   photo can join the selected collection, or a row's status can change
   into the active tab, and the rebuilt node then contains rows that
   were never displayed — admitted by a node-level comparison because
   the node id matches. Node ids name a *bucket*; the invariant is about
   *rows*. Prediction ids are the exact granularity the promise is made
   at, they are already unique (`db.py:866`), and they need no encoding.
   For a `node_id` request the same rule applies with `server_members` =
   the named node's own rows ∩ scope: `member_prediction_ids` travels on
   that shape too, and the node id names which card was clicked rather
   than which rows to write.

   Without the equality check the cache transition below silently widens
   the click: if the resolver resolves `name:blue tit` → `taxon:13094`
   between the GET and the POST, a group `B` that was already
   `taxon:13094`-keyed rendered as its **own separate card**, and a POST
   that mutated the newly-merged component would accept or reject `B`'s
   rows — rows the user could see on screen, but not as part of the card
   they clicked. That is corollary 1 violated in the time dimension, and
   the invariant wins: `B` makes `server_members` differ from the frozen
   membership, so the click is **refused** with `B`'s rows named in
   `joined_prediction_ids` and nothing is written. The client tells the
   user duplicates appeared and offers a reload, rather than leaving a
   card that looks resolved next to a duplicate that is not — and rather
   than proceeding on a short list it cannot distinguish from a lie.

   For a `node_id` request the server resolves exactly the named single
   node under the scope tuple, without any component expansion —
   matching the per-node fallback the filtered view rendered.

   **Legacy payload shapes keep legacy scope.** Where the client sends
   only a `prediction_id`/`group_id` — a Review page loaded before this
   ships, or a deep-link button — the server keeps **today's**
   behaviour exactly: the clicked prediction plus, if grouped, its own
   group's siblings, restricted to that prediction's
   `classifier_model` (`db.accept_prediction` as it stands). It is not
   reinterpreted as an unfiltered taxon card. That page rendered those
   rows as a per-model card and offered a per-model click; upgrading the
   payload's meaning server-side would let a stale click resolve another
   model's rows the old UI displayed as a *separate* card — the same
   unseen-mutation failure as the cache-transition case, arriving
   through version skew instead of through time. Legacy shapes carry no
   frozen membership and no scope tuple, so there is nothing to bound a
   widened interpretation with; the only safe reading is the narrow one
   the old UI promised. Merged-card semantics require the new payload.
2. **Enumerate photos from the resolved (filtered) component.** The
   candidate photo set is the union of every member group's/singleton's
   photos *within the resolved card* — not the clicked group's photos, and
   not the unfiltered full-workspace component. This fixes the transitive
   case: if the displayed card is {A: photos 1-2, B: photos 2-3, C:
   photos 3-4}, accept iterates photos {1, 2, 3, 4}. It also enforces the
   filter's promise: a hidden group D that a collection filter excluded
   from the GET is excluded from the mutation too, even if its taxon and
   photos would otherwise have joined the component.
3. **Sibling pass, taxon-matched, per photo, within the resolved scope.**
   The pass differs by request shape, in order to honour what §2 already
   promises for `node_id` resolution ("the server treats the card as
   exactly that single node … without any component expansion"):
   - **`card_id` request (merged-card view — no client-applied
     predicate; server-applied scope may still be active).** For each photo in the
     resolved component's union, find predictions on that photo whose
     taxon key matches the card's `taxon_key`, from **any** classifier
     model that was in scope for the GET (i.e., predictions the user's
     filter would have surfaced), restricted per model to its latest
     `labels_fingerprint` (reuse the latest-fingerprint subquery from
     `accept_subject_species`), **and then intersected with
     `server_members`** before anything is written; each survivor is
     accepted via the existing `_accept_for_photo` primitive. This is
     what closes the BioCLIP-vs-iNat21 duplicate on the motivating case
     and carries acceptance across A→B→C in a transitive component.

     The intersection is not belt-and-braces here, it is load-bearing —
     and note which set it is against. The scan's predicate is "same
     taxon, on a card photo, in scope", which is *nearly* the component
     edge rule but not identical to it, so bounding it by
     `server_members` is what keeps the scan a member-enumerator rather
     than a second, looser definition of the card. It may only ever
     confirm rows the rebuild already put in the component; it may never
     introduce one. An earlier revision bounded it by
     `member_prediction_ids` instead, which was the right instinct
     applied to the wrong set: the client's list is a claim, not a fact,
     and a mutation step must not take its row bounds from a claim (§2,
     "The precondition is verified against a set the client did not help
     build"). The `name:`→`taxon:` case this bound was introduced for —
     group `B` rediscovered through a photo it shares with a member —
     never reaches the scan now, because `B`'s presence in
     `server_members` already made step 1 refuse the POST with 409.
     Neither the scan nor the client may extend the card (§2, card
     axis), and since equality has already been checked,
     `server_members` and the frozen membership are the same set by the
     time the scan runs.

     **The scan is not restricted to `pending` rows** — that is
     corollary 4's second half. An earlier revision filtered the sibling
     scan to `status = 'pending'`, which meant accepting a card holding
     a rejected member left that member rejected and produced a card
     whose badge said "accepted" over a row that was not: the card
     entered `mixed` as a *result of its own accept*. Dropping the
     status predicate does not widen the mutation, because the status
     predicate was never what bounded it — the bound is the resolved
     component intersected with the scoped row set (step 1) and its
     photo union (step 2). Every row the scan now additionally reaches
     is already a member of the card the user clicked: for a `card_id`
     request, any same-taxon row on a card photo is by construction an
     edge into the component (§2, card axis), so "taxon-matched rows on
     the card's photos" and "the card's members" are the same set. The
     scan enumerates members; it never extends the card. The user is
     told before clicking: a card with a member in the opposite terminal
     state renders as `mixed` with the breakdown in the badge and its
     buttons labelled "Accept all" / "Reject all" (§2, "Client
     changes"), so the reconciliation is stated, not silent.

     *Undo restores prior status, not `pending`.* A total accept makes
     today's undo wrong. `_apply_undo`'s `prediction_accept` branch does
     not restore what each row was before: it flips every
     `accepted`/`rejected` row in the touched
     `(detection, classifier_model, labels_fingerprint)` scope to
     `alternative` and promotes the top-confidence row to `pending`
     (`db.py:18925-18952`). That was survivable while accept only ever
     touched pending rows and their alternatives. Once a card action
     spans members that were already terminal, a blanket reset **erases
     decisions this action did not make** — undoing the accept of a
     `{pending, accepted}` card would knock the pre-existing accepted
     member back to pending, and undoing a reconciling accept would lose
     the fact that a member had been rejected. Both are the status-axis
     form of the same sampling error corollary 4 forbids, one step later
     in time.

     So the accept records **each touched row's prior status** in the
     `prediction_accept` history item payload (`prediction_id` →
     `pending` | `accepted` | `rejected` | `alternative`), and undo
     restores exactly those.

     **The capture point is not `affected`, and this matters.** An
     earlier revision said the snapshot could reuse the set
     `_accept_for_photo` already walks (`affected`, `db.py:17190-17193`).
     It cannot: `affected` is created *inside* `accept_prediction` at
     line 17190 and only ever gains the row `_accept_for_photo` was
     called on (`this_pred_id`). The **sibling/alternative demotion runs
     earlier and separately** — `accept_prediction` flips every
     pending/alternative row sharing the accepted row's
     `(detection_id, classifier_model, labels_fingerprint)` to
     `rejected` in its own loop at `db.py:17137-17162`, before
     `affected` exists, and never appends to it. A row-by-row undo built
     from `affected` alone would therefore leave exactly those siblings
     `rejected` after undo — the blanket-reset branch it replaces
     happened to cover them precisely *because* it reset a scope rather
     than a row list, so switching to row-by-row without widening the
     capture would be a regression, not a fix.

     The rule, stated so it cannot be missed at implementation time:
     **every row whose status this call writes is snapshotted before the
     write that changes it, wherever in the call that write lives.**
     Concretely that is two capture sites, not one:

     1. `accept_prediction`'s sibling loop — read each sibling's current
        `COALESCE(pr_rev.status, 'pending')` in the same `SELECT` that
        already enumerates them (`db.py:17145-17158` selects `pr.id`;
        it selects the coalesced status too) and record it *before* the
        `INSERT … ON CONFLICT … SET status = 'rejected'` fires.
     2. `_accept_for_photo` — the accepted row's own prior status, read
        before `update_prediction_status(this_pred_id, "accepted")`.

     Both feed one snapshot map carried on the returned result alongside
     `affected` (a sibling demotion tags no photo, so it has no place in
     `affected`'s tag-oriented entries and should not be forced into
     one), and the accept API writes that map into the
     `prediction_accept` payload. `_apply_undo` then replays it row by
     row instead of resetting a scope. Undo becomes an exact inverse:
     the pre-existing accepted member stays accepted, a reconciled
     member returns to `rejected`, the pending member returns to
     `pending`, and the sibling alternatives the accept demoted return
     to `alternative` (or to `pending`, if that is what they were) —
     because that is what the snapshot recorded, not because a scope
     reset guessed. Legacy history entries
     written before this ships carry no snapshot; they keep the existing
     blanket-reset branch, selected on the payload's shape, so no
     migration or backfill is needed. The user-visible action
     description still names what the click overrode (e.g. "Accepted 3
     predictions (1 previously rejected)"), because the badge on a
     `mixed` card promised a destructive click and the history should
     say the same thing.

     `prediction_reject` remains non-undoable (`_NON_UNDOABLE`,
     `db.py:18756`) — unchanged by this design, and the reason "Reject
     all" is offered on a `mixed` card at all: it is the only way to
     revise an accept-vs-reject contradiction from Review.
   - **`node_id` request (client-filtered view, per-node fallback).** The
     mutation touches **only the named node's own rows** — no cross-model
     sibling scan, no expansion onto other visible nodes, even for a
     photo the node shares with a visible sibling node that has the same
     taxon. Concretely: `_accept_for_photo` is called exactly on the
     node's own `(photo_id, prediction_id)` set — **all** of it,
     whatever each row's current status, so a single node whose own rows
     disagree also reconciles in one click — and the cross-model
     taxon-matched sibling scan of the `card_id` branch is skipped. The
     visual contract of the per-node fallback is "each card is its own
     click" — the client rendered visible nodes A and B as separate
     cards precisely because a filter made component-wide expansion
     unsafe (§2 "Why the fallback matters for privacy"), and a `node_id`
     accept that reached across the two visible nodes on their shared
     photo would (a) mutate a different card the user did not click and
     (b) leave that sibling card partially resolved — its non-shared
     rows still pending — recreating the exact duplicate-card bug the
     design exists to eliminate, just re-cast between two visible nodes
     instead of between two rendered cards. The sibling node B remains a
     separately clickable card whose own accept touches only its own
     rows; two clicks resolve two cards, symmetric with what the user
     sees. Once every filter is cleared, subsequent Review loads issue
     `card_id` requests again and component-wide expansion resumes.

   The bifurcation only concerns the *sibling scan*: step 1's
   `_apply_visual_to_rules` handling, step 2's per-photo enumeration
   from the resolved membership, and step 4's undo recording all apply
   identically to both request shapes. Under a `node_id` request, step
   2's "resolved (filtered) component" degenerates to that one node's
   own photos, so step 3's `_accept_for_photo` loop already visits only
   the correct photo set — skipping the cross-model scan is the single
   behavioural difference.

- Matching is by `(photo_id, taxon_key)`, not `detection_id`. This covers
  models that classified detections from different detectors (different
  detection rows for the same photo), and it is safe for multi-species
  photos: two birds of *different* species have different taxon keys and are
  untouched. The sibling scan follows the same edge rule §2
  ("Within-run subject partition is authoritative on the edge") uses to
  build the component in the first place — it only broadens **across
  runs**, i.e. rows whose run key (§2, "Run key") is disjoint from the
  clicked row's — never within the run. Within the clicked row's
  own run, the group-siblings loop `accept_prediction` already runs
  (`pr.classifier_model = ?` on the clicked row's own `group_id`)
  remains the only same-run reach — **bounded to the resolved node, per
  the next bullet; "preserved" here means preserved in *reach*, not in
  its two-column `WHERE`** — so the classifier's own
  similarity-grouping decision — one real Blue Tit box in `group_id` A,
  one false-positive Blue Tit box in `group_id` B — is preserved: the two
  remain separately reviewable cards and one accept does not resolve the
  other. This is why the `(photo_id, taxon_key)` sibling scan is safe as
  a *keyword* consideration too: two same-species detections on one photo
  collapse into one photo-level keyword regardless of which of the two
  cards accepts it (the keyword write is idempotent per taxon, §4), so
  the *keyword* granularity is intentionally coarser than the *review
  status* granularity — accepting one card tags the photo, accepting the
  other is a status-only idempotent no-op on the keyword, and rejecting
  one leaves the other's tag in place through the liveness clause.
- **The group-siblings loop is constrained to the resolved node — it is
  bounded by `server_members`, not by `(group_id, classifier_model)`
  alone.** `accept_prediction`'s built-in group loop enumerates its
  siblings by `WHERE pr_rev.group_id = ? AND pr.classifier_model = ?`,
  but the node identity §2 splits cards on is the four-tuple
  `(classifier_model, labels_fingerprint, group_id, species_key)`. For a
  **legacy `group_id` collision** — pre-Phase-0 rows where two bursts
  happen to share `group_id` and `classifier_model` but disagree on
  `labels_fingerprint` or `species_key` — those bursts render as **two
  separate cards** (§2, "legacy-collision split"), yet the two-column
  loop above would still reach every row in the shared bucket. If the
  card mutation invoked `accept_prediction` on the anchor without
  narrowing the loop, one card's accept would silently accept the
  sibling card's rows too, defeating the split. Note that the loop is
  the *only* remaining same-run reach precisely because it is bounded;
  the preceding bullet's "preserved" is about which rows the classifier's
  own grouping decision makes reachable, not about leaving the query
  unconstrained.

  **The primitive for this already exists — PR #1489 added it.** That PR
  gave `accept_prediction` a `prediction_ids` scope list and moved the
  group expansion behind an `_in_scope(photo_id, pred_id)` check that
  tests row membership *independently* of photo membership ("a group
  member's photo being in scope does not make every row that member
  carries in scope"). The card mutation therefore does not need a new
  code path: it calls `accept_prediction(anchor_id,
  prediction_ids=server_members)` and the expansion is confined to the
  resolved node's rows by construction. Two consequences worth pinning,
  because they are interlocks rather than coincidences:

  1. #1489 also made expansion skip rows whose status is in
     `Database.DECIDED_PREDICTION_STATUSES`, which on its own would
     break corollary 4 — a card accept over a `{pending, rejected}`
     card must reconcile the rejected member, and expansion-discovery
     would skip it. It does not break, because #1489 exempts *rows the
     caller named*, and under this design the caller names the entire
     membership. Passing `server_members` is what makes the card
     mutation's reconciling semantics and #1489's
     don't-resurrect-a-rejected-sibling rule both hold at once; a future
     change that stopped passing the full membership would silently lose
     the reconciliation.
  2. #1489's undecided-only narrowing is *necessary but not sufficient*
     here. It stops expansion from re-flipping decided rows; it does not
     stop it from reaching an **undecided** row belonging to the other
     half of a legacy `group_id` collision. Only the explicit
     `prediction_ids` bound closes that, which is why this design
     requires it rather than relying on #1489's status filter.

  Driving the accept by iterating `server_members` directly and calling
  `_accept_for_photo` per row is equally admissible; the invariant is
  that no card mutation writes a row outside `server_members`, however
  the primitives underneath are stitched. The same bound applies
  verbatim to reject's group loop. Under a `node_id` request the bound
  is trivially tighter (the node's own rows are the entire mutation set
  by definition, §3), so this rule is only load-bearing for `card_id`
  requests over legacy collisions — but stating it once here means any
  future refactor that reintroduces an unscoped
  `accept_prediction(anchor_id)` as the driver still cannot re-open the
  hole. Phase 3 gains a **legacy-collision cross-card fixture**: two
  nodes share `(group_id, classifier_model)` but differ on
  `species_key`, they render as two separate cards, and accepting the
  first card touches **only** its members — the second card's rows in
  the same bucket remain untouched, including any that are still
  `pending` and therefore invisible to #1489's status filter.
- Taxon keys are computed in Python via §1's helper (the candidate set —
  the card's member rows on its union photos, in any status — is bounded
  by the card size), so no SQL-side taxonomy join is needed.
- **No need to iterate to closure.** The card component is fully materialized
  before the accept fires (§2 already builds it via connected components
  over `(same taxon_key, overlapping photos)`). Iterating photo-by-photo
  over the pre-computed union is closure — sibling matches on photo 4 are
  reached even though the clicked group only covered photos 1-2. A later
  push that introduces a *new* group after accept fires does not retroactively
  join this card; that new group appears as a fresh pending card on the next
  Review load, which is the intended behavior. A group that joins the
  component *between* the GET and the click does **not** get that
  treatment, because there the user has a click in flight against a card
  that no longer exists: the rebuild finds it, the membership check
  fails, the POST is refused 409 with the newcomer in
  `joined_prediction_ids`, and the merge lands on the next load
  (step 1).
- **Undo:** every member accept goes through `_accept_for_photo`, whose
  `affected` entries — including status-only no-ops — feed the
  edit-history/undo machinery, and the accept additionally snapshots the
  prior status of the rows `accept_prediction`'s sibling loop demotes,
  which `affected` does *not* contain (see "The capture point is not
  `affected`" in step 3). The
  `prediction_accept` history entry therefore covers *every* member row
  across the component, not just the clicked group's, and restores each
  one to **the status it held before the click** — pending members to
  `pending`, a member the accept reconciled out of `rejected` to
  `rejected`, a member that was already `accepted` to `accepted`, and
  demoted alternatives to `alternative`. See "Undo restores prior
  status, not `pending`" in step 3 for why the existing blanket reset
  cannot survive a total accept and what the payload has to carry.

**Reject.** Mirror logic: rejecting a card rejects all member predictions
(all models) across the same union photo set and card taxon — including
members already `accepted`, symmetrically with accept's reconciliation
above, so that "Reject all" on a `mixed` card leaves the card unanimous
and the card's badge never outruns its rows. Today reject
is per prediction/group; it gains the same sibling pass, scoped to the
same component-wide photo union so transitive cards resolve completely,
and it carries the same full scope tuple as accept
(`rules`/`collection_id`/`model`/`labels_fingerprint`/`min_confidence`/`status`/`visual`)
so that a rejection issued from a filtered view never touches rows the
user could not see, including rows on photos outside the active
visual clause's match set.

*Rejecting a previously-accepted member has to retract that accept's
tag.* Reject is a pure status flip today (`api_reject_prediction`,
`app.py:15780-15827`) — it writes no keyword and removes none, which is
correct while reject only ever applies to pending rows. Once "Reject
all" can overrule an accepted member, a status-only flip would leave the
card badged "Rejected" while the photo still carries the species keyword
that accept wrote — a card saying one thing and the photo's metadata
another, which is the same no-black-boxes violation as the badge hole
this section closes. So a reject that flips an `accepted` member also
untags that member's taxon keyword on that member's photos, through
`untag_photo` + `remove_pending_changes(photo_id, 'keyword_add', name)`
and recorded as an ordinary **`keyword_remove`** history entry —
separate from the non-undoable `prediction_reject` status entry, so the
destructive half of a reconciling reject is undoable even though the
status flip is not. Rejecting a pending member is unchanged: nothing
was tagged, nothing is retracted. `remove_pending_changes` defaults to
the **active workspace only** (`db.py:18630-18649`), so the call above is
written cross-workspace — see "The pending queue is workspace-scoped and
the association is not", below, for why a workspace-local cancellation
lets another workspace's stale queue write the retracted keyword back to
the sidecar.

**A card mutation writes every member status before it decides any
keyword effect.** This is a general rule about card mutations, not a
patch on "Reject all", and it binds accept as much as reject. A card
mutation runs as two phases inside one transaction, in this order.
(Lettered to keep them distinct from the numbered steps of the accept
algorithm above and from the numbered implementation phases below:
these two are halves of one mutation, at runtime.)

- **Phase A — statuses.** Write the new status of *every* row in the
  mutated set (`server_members = resolved component ∩ scope`, step 1 —
  equal to `member_prediction_ids`, and holding the statuses `observed`
  recorded, on any request that got this far — step 1's precondition
  already 409'd the ones where either was untrue), reconciling flips
  included. No keyword is written or
  removed and no keyword-related predicate is evaluated.
- **Phase B — keyword effects.** Only after the last Phase-A write:
  resolve the card's single keyword over the union of member photos (§4,
  "Resolved once per mutation, not once per photo") — the *written*
  string on an accept, and the string the accept's disclosure names —
  evaluate the retraction predicate, perform the tag on accept or the
  per-photo enumeration untag on reject (below), and compute the
  disclosure counts, all against the post-Phase-A state. The keyword
  resolution belongs in Phase B for the same reason the retraction
  predicate does: it reads which member photos already carry a
  taxon-matched keyword, and on a reconciling accept that set is only
  final once every member's status is written.

  *The single-string rule bounds writes and disclosure, not removals.*
  §4 case 2 — member photos carrying two or more different taxon-matched
  keywords from earlier accepts, an admitted residual whose merge is a
  Keywords-page follow-up — is exactly the case where resolving reject
  to one spelling would leak: the retraction would flip every member to
  `rejected` while leaving the other accept-owned synonym (and its
  sidecar change) on the photos that carry it, the same
  badge-disagrees-with-metadata failure this section exists to prevent,
  this time bounded by an addition rule reject has no reason to obey.
  Retraction is therefore *per photo, over every accept-owned
  taxon-matched association*: for each affected photo, enumerate every
  `photo_keywords` row whose `keywords.taxon_id` resolves to the card's
  taxon (through the same `taxa.inat_id` → `taxa.id` translation §4
  precedence-1 uses, so synonym rows are found even when their id
  differs from the resolved-once card keyword), and retract each one
  whose folded `source = 'accept'` that survives the liveness predicate
  below. §4 case 2's stated invariant is that accept "may leave other
  synonyms already on member photos in place" — precisely those
  synonyms are the set a later reconciling reject must retract, or the
  card's badge outruns its own metadata trail one accept later. When
  that set has more than one member on the same card, the disclosure
  names each string rather than picking a representative:
  `untagged "Blue Tit" and "Eurasian Blue Tit" from 3 photos`.

  *`name:`-keyed cards enumerate by folded keyword name, not
  `taxon_id`.* An unresolved custom label has no `taxa` binding, so the
  `taxon_id` join above finds no row — and the accept side already
  knows this: §4 precedence 2 states that "`name:`-keyed rows
  (unresolved) keep writing their raw label, as today". Under #1488
  that raw-label row is stamped `source = 'accept'` all the same, so a
  retraction driven *only* by `taxon_id` would flip every member to
  `rejected` without finding either the keyword or its pending
  `keyword_add`, and the card would badge "Rejected" over a photo still
  carrying the accept-owned tag — the same badge-disagrees-with-metadata
  failure the retraction rule exists to prevent, this time arriving
  through the fallback path §1 deliberately leaves in. So the
  enumeration takes a **second clause for cards whose key is
  `('name', L)`**: for each affected photo, additionally enumerate
  every `photo_keywords` row whose `keywords.name` folds equal to the
  card's canonical folded label (the same `_fold` §1 uses to build the
  key), and retract each one whose folded `source = 'accept'` that
  survives the liveness predicate below. The two clauses do not overlap
  by construction — the key is a card-level property built once in §1
  and every accepted member of the card wrote its keyword under the
  same clause of §4 — so each retraction clause finds exactly the
  associations its half of §4 could have written. Case 2's residual is
  taxon-only: a `name:`-keyed card has no taxonomy over which two
  synonyms could exist, so no per-photo enumeration extra is needed
  beyond the fold-equal match.

  The two clauses together define one relation, and the rest of this
  section is stated over it rather than over taxa: **a card's key
  *claims* a `photo_keywords` row** when that row is what the card's
  own half of §4 would have written or reused — taxon-resolved rows
  for a `('taxon', T)` card, fold-equal rows for a `('name', L)` one.
  Stating retraction at the level of *card identity* rather than of
  taxon ids is not a stylistic preference: it is the same level §2's
  frozen-membership precondition and §3's scope tuple are already
  stated at, and it is the only level at which the `name:` fallback
  and the taxon path are one rule instead of two that have to be kept
  in step by hand. The non-overlap noted above is a statement about
  which clause a *single card* uses; it is emphatically not a
  statement that a given `photo_keywords` row is claimed by only one
  card, which the liveness rule below turns on.

  *Persisting the exact accepted `keyword_id` on the
  `prediction_review` row was the alternative, and is rejected.* It
  invents a new column to record something recoverable from the same
  fold `('name', L)` was keyed on, and it under-covers the two
  cases the read-time enumeration was already built to catch: rows
  written before the column exists (backfill on legacy data would have
  to fall back to the fold anyway), and case 2's residual synonym rows
  on `('taxon', T)` cards, where an earlier accept wrote a
  taxon-matched keyword whose `keyword_id` this reject would not have
  picked. The read-time key mirrors the accept side and is strictly
  smaller.

Without the split, a row-at-a-time loop evaluates each keyword decision
against a state the same click is about to invalidate. The concrete
failure: "Reject all" on an `{accepted, pending}` card whose two members
assert the same taxon on the same photo. Process the accepted row first
and the liveness clause below sees the still-`pending` sibling as
another live assertion, so it keeps the accept-owned keyword; the
pending row is then rejected and retracts nothing, because that row
never tagged anything. The card ends unanimously rejected with the
keyword still on the photo — exactly the badge-disagrees-with-metadata
failure the retraction rule exists to prevent, manufactured by the
retraction rule's own guard. The predicate is not wrong; *when it is
asked* is wrong.

Accept is not exempt, only accidentally safe today. Its keyword write is
idempotent per taxon (§4) and its `already_has_species` check
(`db.py:17195-17197`) reads keywords rather than statuses, so no
interleaving bites right now. That is a property of the current effect
set, not of the mutation's structure: any keyword effect added on the
accept side — a replace-style accept that strips a superseded taxon's
keyword is the obvious candidate — reinstates the interleaving
immediately. Stating the phase order for card mutations in general makes
that impossible rather than merely unlikely.

*Why not simply exclude the current action's members from the liveness
check.* That is the narrower repair the symptom also admits, and it
fixes one predicate by hand. Phase B holds several state-reading steps —
the retraction guard, the disclosure's "still predicted on 2 photos"
count, the "kept because another live prediction still asserts it"
wording — and each would need its own exclusion list, correctly
maintained, forever, including the next one somebody adds. The ordering
rule makes all of them right at once, and the exclusion behaviour falls
out of it as a consequence: after Phase A the mutated members *are*
rejected, so they no longer count as live. One rule, no list.

*What "another live assertion" means, precisely.* Phase B is where it
runs, so it is worth pinning down: a row in
`predictions`/`prediction_review` on that photo, in **any workspace
that contains the photo — not just the active one** (see the
cross-workspace note below) whose **claim candidates include the
association being retracted** (the relation defined by the two
enumeration clauses above, evaluated scope-free — "The key those pairs
are matched on cannot be §1's", below; for an assertion sharing the
card's key this reads simply as "its key equals the card's", and the
cross-key case is two paragraphs down) and
whose **post-Phase-A** status is `pending` or `accepted`. Both
`rejected` and `alternative` are excluded, and for the same reason:
only top-level, still-in-play assertions can keep an accept-owned tag
alive. `rejected` is out because the reviewer has retired it;
`alternative` is out because it was never a card in the first place —
it is a runner-up in some other row's top-k output that §Edge cases
(*Alternatives*) already excludes from card building and sibling
resolution, so it cannot be treated as an independent assertion here
without contradicting that exclusion. Concretely: a top-k classifier
emits an `accepted` "Blue Tit" row for photo P alongside a runner-up
`alternative` "Blue Tit" row on the same photo; if the user later
rejects the accepted card, an `alternative`-inclusive predicate would
see the runner-up as a live sibling and retain the accept-owned
keyword, so the card would badge "Rejected" while the photo still
carried the tag — the same badge-disagrees-with-metadata failure the
retraction rule is here to prevent, this time manufactured by the
retraction rule counting rows the card itself does not contain. The
liveness predicate is therefore `status IN ('pending', 'accepted')`
and the exclusion of `alternative` is *not* a "same rule stated
twice" with the Phase-A demotion machinery — Phase A only demotes the
mutation's own members, and `alternative` rows are not members of any
card. It is also deliberately *not* restricted to the card's members
in the *other* direction — a `pending`/`accepted` same-taxon row on a
different card, or one the mutation's scope excluded, keeps the
keyword alive and must, which is also why "ignore every sibling"
would be wrong where "evaluate after the writes" is right. And it is
not `get_photos_with_equivalent_species` (`db.py:13874`), which
answers the different question "does this photo already carry an
equivalent species keyword" and is the accept path's idempotence
check.

*Liveness is asked per claimed association, not per card key.* Keying
it on "same §1 key as the card" is right whenever both sides are
`taxon:` keys, and wrong at the seam the `name:` clause just opened,
because the two kinds of key can claim the **same** `photo_keywords`
row: a `('name', 'blue tit')` accept writes the raw label "Blue Tit",
and a `('taxon', 13094)` card whose §4 precedence-1 lookup reuses that
very keyword row claims it too — the row's `keywords.taxon_id` and its
folded name are both satisfied, by different cards. Under card-key
equality each of those two cards treats the other's live assertion as
invisible, and whichever is rejected first strips a tag the other
still asserts: the same badge-disagrees-with-metadata failure, this
time along the key axis rather than the mutation-order axis or the
workspace axis. So the unit of the question is the association, not
the card: a claimed row is retracted only when **no** live assertion
anywhere claims it, where each assertion claims rows under its *own*
candidates by the relation above — which the cross-workspace rule below
then has to make scope-free, since another workspace's key is not
computable from here. This is the second reason the rule is
stated over card identity — a taxon-keyed formulation cannot even
express half of the set it is obliged to check — and it is the reason
the two enumeration clauses had to be folded into one relation rather
than left as a taxon path with a `name:` special case bolted beside
it.

*Cross-workspace, and load-bearing.* `photo_keywords` is a
catalog-global table — `(photo_id, keyword_id)` with no
`workspace_id` column (`db.py:728-732`) — while
`prediction_review.status` is workspace-scoped, keyed on
`(prediction_id, workspace_id)` (`db.py:925-935`). The two rows point
at the same photo but answer different questions: the keyword row is
"does the photo carry this tag at all", and the review row is "did
the reviewer in *this* workspace decide about this prediction". A
liveness predicate restricted to the active workspace would therefore
let a reconciling reject in workspace A strip a globally-stored
keyword that workspace B still `pending`- or `accepted`-asserts
through its own `prediction_review` row for the same photo. The
metadata written to disk would silently contradict the state
workspace B still displays, and A's own disclosure would name a count
of zero live assertions when B has several — the same
badge-disagrees-with-metadata failure the retraction rule exists to
prevent, this time arriving through the workspace axis rather than
the mutation-order axis. The predicate therefore evaluates over every
workspace that contains the affected photo, and **the unit it
evaluates is the `(prediction, workspace)` pair, not the
`prediction_review` row**.

That distinction is the whole rule, because in this schema *pending
state is represented by absence.* `add_prediction` writes no
`prediction_review` row at all unless the caller supplies something
beyond the defaults — `has_review_state` gates the insert
(`db.py:15905-15918`) under the comment "Keeping pending rows out of
`prediction_review` is intentional: absence == pending" — which is why
every read path spells the status `COALESCE(pr_rev.status, 'pending')`
(`db.py:16204`, `16236`, `16634`, `17149`, and a dozen more, including
the `get_predictions` query Review itself renders from). A liveness
query that starts `FROM prediction_review` therefore finds nothing in
workspace B for a prediction B has never been clicked in, and concludes
B holds no live assertion — while B's Review page is at that moment
displaying that prediction as `pending`. That is the **ordinary** case,
not the edge one: most workspaces have never touched most predictions,
so a row-started query under-counts live assertions in nearly every
workspace, and the retraction it is guarding would fire on nearly every
shared photo.

The liveness set is built the way `get_predictions` builds its status
column, one level up. Enumerate the candidate pairs *first*: the
`predictions` rows on the affected photo whose **claim candidates**
(next) include the association being retracted, crossed with every
workspace whose `workspace_folders` make that photo visible — the join
`_photo_in_workspace` performs at `db.py:2121-2129`, generalized from
"the active workspace" to "every workspace containing this photo".
Then `LEFT JOIN prediction_review pr_rev ON pr_rev.prediction_id = p.id
AND pr_rev.workspace_id = w.id` and test
`COALESCE(pr_rev.status, 'pending') IN ('pending', 'accepted')` — the
identical convention, on the identical column, as the query that decides
what the other workspace shows. Phase A's writes are already in the
transaction, so the mutation's own members read `rejected` here exactly
as the phase-ordering rule requires. Retraction happens only when no
pair is live.

*The key those pairs are matched on cannot be §1's, and must not try to
be.* §1 deliberately makes a label's key a function of the label **and
of the row set it is resolved over**: `S(L)` is collected from the
request's rows for `L`, and two of its members resolving to different
taxa abandon the ladder for `('name', L)`. So the key another
workspace's Review is displaying depends on *that* workspace's rows, and
a resolution over the affected photo's rows alone reproduces neither it
nor the card's. Concretely, in the destructive direction: every row on
the affected photo labelled `L` carries `scientific_name = NULL` — the
ordinary state for a run made with no taxonomy file installed (§1) — so
photo-locally `L` falls to `('name', L)` and claims only fold-equal
keyword rows; workspace B's row set also holds a row labelled `L` on a
*different* photo whose stored `scientific_name` resolves to `T`, so B
renders a `('taxon', T)` card, and that card claims the
`source = 'accept'` synonym "Eurasian Blue Tit" this reject is about to
strip out from under it. The mirror — photo-local `('taxon', T)` while B
sees a conflict and keys `('name', L)` — retains a keyword no live card
claims. One defect, both directions.

*Rebuilding each workspace's applicable row set is rejected, and not on
cost.* It is not well defined. A workspace's Review row set is shaped by
**client-applied** predicates — `minConfidence` and the status tab, two
entries of §3 step 1's own scope tuple — which live in whichever browser
tab that workspace is open in and are persisted nowhere. The mutation
reproduces its *own* card key exactly only because the client forwards
its scope tuple with the POST; there is no such channel from another
workspace, and no query can invent one. An unfiltered workspace-wide
rebuild would therefore answer for a filter state B may not be in, while
costing a scan of every prediction row in every containing workspace on
every retraction. Wrong answer at a high price — worth saying plainly
rather than specifying and quietly never building.

*So liveness takes the other option: an association-level relation that
does not move with query scope, and is deliberately a superset of every
scoped one.* Replace "the assertion's §1 key" with the assertion's
**claim candidates**. A live assertion `A` — a `predictions` row with
folded label `L` on the affected photo, paired with a workspace
containing that photo, post-Phase-A status `pending` or `accepted` —
claims a `photo_keywords` row `X` on that photo when **either**

- `fold(X.keyword.name) == L` (the fold §1 builds `('name', L)` with), or
- `X.keyword.taxon_id ∈ Taxa(L)`,

where `Taxa(L)` is the catalog-wide candidate set for the label: every
taxon `Taxonomy.lookup` returns for a distinct non-`NULL`
`scientific_name` carried by **any** row labelled `L`, plus rung 2–3's
result for `L` itself. The relation picks no key; it is the union of the
claims of every key §1 could assign `L` under any scope. That it
contains every scoped relation is immediate — a scoped `S(L)` is a
subset of the catalog-wide one, so §1 returns either `('name', L)`,
clause 1, or `('taxon', T)` with `T` drawn from that subset, clause 2.
The card's *own* key is untouched and stays exact, because the mutation
knows its own scope: the client forwarded it. Two questions, two keys,
and what separates them is that one scope is known and the others are
unknowable.

*The asymmetry is chosen, not tolerated.* A superset relation errs only
toward **keeping** a keyword, and the two errors are not comparable. A
keyword kept when nothing really claims it is announced in the sentence
the retraction rule already writes — "kept 'Blue Tit' — still predicted
in 2 other workspaces (Birds 2024, Archive)" — and the user can remove
it from the Keywords page. A keyword stripped while a live card claims
it is a silent write to a shared sidecar, surfacing in a workspace the
user was not looking at, and it is the failure this whole rule exists to
prevent. Conservatism has a direction here, and this is it.

*Persisting the claim instead cannot answer this one.* Recording which
association each accept wrote is already rejected for the *enumeration*
(above); for liveness it is not merely smaller but inapplicable. Most
live assertions are `pending`, a pending assertion has written nothing
to link to, and under the implicit-pending rule above it does not even
have a `prediction_review` row. Liveness must be derived from the label
— which is precisely why the scoping problem attaches here and not to
the enumeration.

*The label predicate cannot live in SQL, so it does not.* "Any row
labelled `L`" is membership in `_species_match_key`'s equivalence class,
and that fold is NFKC normalization, whitespace collapse, edge-quote
strip, apostrophe fold **and** an ASCII case fold
(`keyword_normalization.py:100-144`, `classify_job.py:40-66`). A
`WHERE species IN (…)` over the affected photo's raw spellings matches
none of those: `predictions.species` is plain `TEXT` with no
`COLLATE NOCASE` (`db.py:865-883` — the only collation-free spelling in
the identity tuple), so a case-equivalent or apostrophe-variant live row
on some *other* photo is silently absent from `Taxa(L)`. That is not a
tolerable narrowing. It is the one thing this relation may not do: the
whole safety argument above is that the relation is a **superset** of
every scoped one, and a lookup that can miss a live row inverts the
error direction from *keeping* a tag nobody claims to *stripping* one a
live card still asserts. Patching the predicate to
`species COLLATE NOCASE IN (…)` fixes only the last of the five fold
steps and is therefore still not a superset — and it buys nothing
anyway, because a `NOCASE` comparison cannot use the `BINARY`-collated
index and degrades to a full index scan (`EXPLAIN QUERY PLAN`:
`SCAN predictions USING COVERING INDEX …`), which is the cost of
projecting the whole vocabulary without the correctness.

*So project the whole vocabulary once and fold it in Python, as §1 does
everywhere else.* The query loses its `WHERE` clause on `species`
entirely:

```sql
SELECT DISTINCT species, scientific_name
  FROM predictions
 WHERE species IS NOT NULL
```

Fold each returned `species` in Python and bucket by fold key; the
result is the *complete* fold-key → distinct-`scientific_name` map for
the catalog, from which every candidate label's `Taxa(L)` is read off
directly. Because one pass answers every label, the memo is now the
whole map per request rather than one entry per label — strictly less
work than the per-label version it replaces, not more.

*Cost, since the whole point was affordability.* The map is
workspace-independent, so it is built once per retraction however many
workspaces contain the photo — the rejected per-workspace option's cost
was per workspace, this one's is once, full stop. It stays affordable
because the row count is not the vocabulary size: on the development
catalog, 162,640 prediction rows carry 4,408 distinct
`(species, scientific_name)` pairs across 3,145 distinct spellings, so
SQL dedups two orders of magnitude away before Python folds anything.
Measured cold on that catalog *without* the index — a full table scan
plus a temp b-tree for `DISTINCT` — the query is ~37 ms; the covering
index on `predictions(species, scientific_name)` removes both, since the
index order already satisfies the `DISTINCT` (`EXPLAIN QUERY PLAN`:
`SEARCH predictions USING COVERING INDEX … (species>?)`, with no
`USE TEMP B-TREE FOR DISTINCT`). That index is the one the previous
draft asked for and it is still the right one — but note it now earns
its keep by making an *unbounded* projection index-only, not by making a
seek fast, so it is required rather than an optimization. Additive, and
per §1's `user_version` caveat created behind a `db_meta` marker or a
PRAGMA check rather than a version-gated migration. Asserting the plan
shape in a test is cheap and worth it: a regression that drops the index
turns every reject into a table scan.

*Rejected: persist the fold as a `predictions.species_key` column.* It
would restore an exact index seek and make the superset property
structural rather than argued. It is not chosen because it puts a
schema change, a write-path change in `add_prediction`, and a backfill
of every existing row on the critical path of a phase that otherwise
touches no prediction storage — to speed up an operation that happens
once per reject click and already costs less than the request's own
overhead. Worth revisiting only if profiling contradicts the numbers
above; the query above is a drop-in for it.

A label whose catalog-wide scientific names genuinely conflict yields
two taxa in `Taxa(L)` and so a wider claim set, which is the
conservative direction again rather than an exception to it.

*The consequence — retraction is rare on shared photos — is correct,
not a number to tune.* If a photo is visible in five workspaces and the
user has only ever reviewed in one, rejecting there leaves four implicit
`pending` assertions standing and the keyword is kept. That is what the
other four workspaces are in fact asserting, and stripping the tag would
contradict what each of them displays. It does mean the disclosure has
to say *where* the surviving assertions are, in the user's terms:
"kept 'Blue Tit' — still predicted in 2 other workspaces (Birds 2024,
Archive)", not the same-workspace wording "still predicted on 2 photos".
A bare count would answer "why was this kept" with a number the user
cannot reconcile against the workspace in front of them — the cheap
proxy the transparency rule forbids — and naming the workspaces is also
the only affordance that tells them where to go to finish the job.

*Rejected alternative: count only workspaces that hold an explicit
`prediction_review` row.* That is the failing query with a
rationalization attached. It makes a destructive metadata decision turn
on whether some unrelated workspace happens to have *written a row* —
a fact invisible in both workspaces, unrelated to whether either asserts
the taxon, and flipped by unrelated actions like a group-id backfill that
sets `has_review_state` without changing any status. The other
alternative — giving `photo_keywords` its own `workspace_id` column
and duplicating each taxon keyword per workspace — is rejected for the
same reason §4 precedence-1 reuses a taxon-matched keyword globally in
the first place: keywords back XMP sidecars on disk, which have no
concept of a workspace, so a per-workspace tag row would either write
one workspace's synonyms to a shared file or split the file per
workspace, both of which break the "one tag per photo per taxon"
invariant the tagging pipeline is built on. The cross-workspace check
preserves that global rule and lets retraction stay per-photo, which
is the same shape the Phase-B enumeration above uses.

*The pending queue is workspace-scoped and the association is not.*
The liveness fix above decides **whether** to retract; this decides
what "retract" has to touch, and it is the same asymmetry one table
over. `pending_changes` carries a `workspace_id` (`db.py:810-818`) and every
accessor is scoped to it — `get_pending_changes` and
`count_pending_changes` filter on `self._ws_id()`, and both
`queue_change` (`db.py:18587`) and `remove_pending_changes`
(`db.py:18630`) take an optional `workspace_id` that **defaults to the
active workspace**. And the queue is a *delta* list, not a desired-state
one: `sync.py:155-243` writes exactly the queued `keyword_add` /
`keyword_remove` values into the sidecar rather than re-deriving the
photo's keyword set from the DB. So an accept in workspace A both writes
the global `photo_keywords` row *and* queues A's own `keyword_add`; a
reconciling reject issued from workspace B that deletes the global row
and cancels only B's queue leaves A's add sitting there. The next time A
syncs, it writes into the sidecar a keyword that no prediction anywhere
still asserts and that the DB no longer contains — disk contradicting
the catalog, which is the failure this section exists to prevent,
arriving after the click that was supposed to fix it and in a workspace
the user was not looking at.

A retraction is therefore global in both halves, matching the table it
is retracting from:

- **Cancel every matching pending `keyword_add` for that
  `(photo_id, keyword name)` in every workspace**, not just the active
  one — `remove_pending_changes` already takes `workspace_id`, so this
  is a loop over the workspaces holding such a row (equivalently, the
  same delete with the `workspace_id` clause omitted), not a new
  method. Deliberately keyed on the row's own `(photo_id, value)` rather
  than on the workspaces currently containing the photo: folder
  visibility can change after an add is queued, and a stale add in a
  workspace that no longer sees the photo still writes the sidecar.
- **Queue the `keyword_remove` in every workspace that contains the
  photo**, deduplicated by `queue_change`'s existing already-pending
  check (`db.py:18608-18613`, which returns `None` rather than inserting
  a duplicate). Queuing it only in the acting workspace is the tempting
  smaller fix and it strands the removal: the association was global,
  the sidecar is one file, and because sync is delta-driven, a removal
  sitting in a workspace the user never syncs never reaches disk — so
  the tag survives in the one place every workspace reads it from. The
  sidecar removal is idempotent (`remove_keywords` on a term the file
  does not contain is a no-op), so whichever workspace syncs first
  strips the term and the rest cost one no-op write, and meanwhile each
  workspace's pending list truthfully shows a removal that is in fact
  happening to a photo it displays.
- **Do not lift `_queue_keyword_remove`'s cancel-instead-of-queue
  shortcut to the global rule.** Within one workspace, `app.py:8234-8250`
  queues a `keyword_remove` only when no matching pending `keyword_add`
  was cancelled — sound there, because a surviving add means that
  workspace never synced it, so there is nothing on disk to remove.
  Read globally, "some workspace's add was cancelled, therefore skip the
  removal" is false: A can accept, sync (clearing A's row, writing the
  term to the sidecar), and *then* B can accept the same taxon on the
  same photo from a different card and queue its own add. Cancelling B's
  add and skipping the removal leaves the term on disk permanently. The
  global rule cancels unconditionally and queues unconditionally; the
  cost of the extra no-op is a write, and the cost of the shortcut is a
  wrong sidecar.
- **Every restore of `(photo_id, keyword name)` cancels matching
  pending `keyword_remove` in every workspace, too — not just the
  active one.** The queue-side rule above puts a `keyword_remove` into
  every workspace containing the photo, and each of those queues has
  its own sync lifetime; leaving the restore side workspace-local
  undoes exactly the symmetry the queue side just established.
  Concretely: A syncs first and its remove reaches the sidecar, then
  the user either undoes the reject or hand-retags the same term in A
  through `tag_photo`. B's queued `keyword_remove` still sits,
  unchanged, and the next time B syncs it strips the restored tag back
  off the sidecar — the same badge-disagrees-with-metadata failure the
  queue-side rule fixes, arriving through the restore path a minute
  later and in a workspace the user was not looking at. So every entry
  point that restores `(photo, keyword name)` loops
  `remove_pending_changes(photo_id, 'keyword_remove', name,
  workspace_id=…)` across every workspace, keyed on the row's own
  `(photo_id, value)` for the same folder-visibility reason the
  queue-side cancel bullet gave. The paths in scope:
  `_apply_undo`'s `keyword_remove` branch (`db.py:18956-18970`, which
  today calls `remove_pending_changes` unqualified and so picks up the
  active workspace only); `_queue_keyword_add`'s
  cancel-instead-of-queue shortcut (`app.py:8225-8232`); and any
  subsequent card accept whose Phase-B keyword resolution writes the
  same term into a photo whose global `photo_keywords` row was
  previously stripped — under §4 precedence 1 that write is what makes
  the row exist again, so every other workspace's stale
  `keyword_remove` must be cancelled at the moment of the write, for
  the reasons above. *A rejected alternative — a single global
  `pending_changes` row with `workspace_id = NULL` for the removal* —
  would need a null-workspace value on a column every accessor scopes
  on (`get_pending_changes`, `count_pending_changes`, and both
  writers), and per-workspace pending views exist so the sync summary
  answers questions about *this* workspace's outstanding writes. A
  global-scoped row would leak into all of them, or force each
  accessor to grow a second clause; cancelling across workspaces on
  the restore side keeps the existing scoping contract intact and
  costs one extra small `DELETE` per restore. The three paths above
  are the closure clause for restores, in the same shape as the
  entry-point table below is the closure clause for status writes: a
  path that restores a `(photo, keyword)` association without the loop
  is a bug, not a gap.
- **And a queued `keyword_remove` is re-validated against the catalog
  when it is applied, rather than trusted because it was queued.** The
  restore-side loop above enumerates three paths and asserts the
  enumeration is closed; that is the right rule, and it is still a
  closure clause maintained by hand over a queue whose copies have
  independent lifetimes. The tension underneath is structural and is
  not going away: `sync.py` is *delta*-driven (`sync.py:155-243` writes
  the queued values rather than re-deriving the photo's keyword set),
  `pending_changes` is workspace-scoped (`db.py:810-818`), and
  `photo_keywords` is global — so *any* multi-workspace queueing
  produces copies that can outlive the condition that justified them,
  and a copy the loop never reached is still a live instruction to
  strip a term the catalog says the photo has. The loop misses at
  least two cases by construction: a removal queued while the photo
  was visible in a workspace that has since lost the folder (the same
  stale-visibility argument the queue-side bullet uses to key on
  `(photo_id, value)`), and any future restore path added without the
  loop — precisely the failure the enumeration can only *forbid*, not
  prevent. So the invariant is also stated at the point of the write:
  where `sync.py` collects a photo's `keyword_remove` deltas, it drops
  any whose value still matches a live `photo_keywords` association on
  that photo — compared on `keyword_match_key`, the normalization the
  sidecar writer already uses — and deletes the pending row instead of
  writing it. The catalog is authoritative for *whether the photo has
  the keyword*; the queue only records *when to tell the disk*, and a
  delta that contradicts the catalog at apply time is stale by
  definition, whatever workspace holds it and whatever restored it.
  The symmetric hazard on the add side is the same check inverted —
  apply a `keyword_add` only while the association exists — which is
  what every add producer already means: `audit.py:580` re-queues adds
  *because* the catalog holds an association the sidecar lacks.
  **Two exemptions, both already visible in `sync.py`'s structure.**
  `paired_removes` — a remove whose match key is re-added in the same
  batch — is a spelling normalization whose purpose is to rewrite a
  variant on disk *while* the association survives, so re-validation
  must not drop it and it keeps its flat-only path unchanged;
  `keyword_remove_flat` is exempt for the identical reason
  (`repair_duplicate_photo_species` queues it exactly when the
  hierarchical association is meant to stay). The two rules are not
  redundant and neither subsumes the other: the restore-side loop is
  what keeps every workspace's *pending list* truthful in the window
  before its next sync — a workspace advertising "remove 'Blue Tit'"
  for a photo whose catalog row says otherwise is the cheap-proxy
  substitution the transparency rule forbids, one surface over — and
  the apply-time check is what keeps the *sidecar* correct for the
  copies no cancellation reached.

**This cancellation is not a second retraction rule — it is downstream
of the first, and inherits #1488's lattice by construction.** It runs
only for the `(photo, keyword)` pairs the retraction predicate below
actually retracted: rows whose folded `photo_keywords.source` is
`'accept'`, no live assertion left catalog-wide. It never inspects
`pending_changes.value` to make a provenance judgement of its own — it
could not, since a queued change records a keyword string and nothing
about who asked for it. The case that makes the distinction load-bearing:
an accept in A stamps `'accept'` and queues an add, then the user
hand-tags the same keyword in workspace B, whose `tag_photo` folds the
row up the lattice to `'manual'`. A later reject retracts nothing —
so it cancels nothing, and A's queued add still writes a keyword the
user explicitly asked for. A parallel "cancel pending adds for the
rejected taxon" rule, written beside the retraction rule instead of
under it, would have cancelled that add and silently dropped the user's
own keyword from the sidecar. One decision, two effects.

**Retraction requires provenance, and provenance cannot live in the
edit log.** Stripping a keyword is only safe if we know the *accept*
put it there; a photo can carry "Blue Tit" because the user typed it,
because a sidecar import brought it in, or because an earlier accept on
a different card wrote it. The obvious test is the `no_tag` bit an
accept records when the photo already carried an equivalent species
(`db.py:18849-18866`) — but that bit lives in a `prediction_accept`
history item, and `_prune_edit_history` permanently deletes entries past
`max_edit_history` (default 1000, `db.py:19542-19551`). A tag written by
an accept and a tag merely confirmed by one become indistinguishable the
moment their entry ages out, and `photo_keywords` on `main` today is
`(photo_id, keyword_id)` and nothing else (`db.py:728-732`) — PR #1488
is adding the provenance column the next bullet builds on. A bounded
log cannot back an unbounded invariant, so this design does not ask it
to. Two rules, split by what is actually knowable:

- **Prospectively — persist provenance, on the column PR #1488 is
  already adding.** `photo_keywords.source` is not this design's column
  to invent: PR #1488 (`rethink-wildlife-tag-removal`) adds the nullable
  `source TEXT` column right now, with the contract "`'manual'` means a
  person explicitly added this; `NULL` means unknown (legacy rows,
  scanner/XMP import, model output)", stamped by
  `tag_photo(..., source='manual')` and **never downgraded** — its
  upsert is `source = COALESCE(excluded.source, photo_keywords.source)`.
  This design **adopts that column and that vocabulary** rather than
  defining a parallel one. An earlier revision of this section proposed
  `'accept'` / `'user'` / `NULL`; `'user'` is renamed to #1488's
  `'manual'` (same meaning, one spelling), and `'accept'` is added as a
  third value written by the accept tag path. (This is still not the
  `prediction_review.group_pid` column §2 rejected. That one would have
  bought *zero* coverage because the information it needed was never
  written anywhere; this one records something the accept path knows at
  write time and nothing else can reconstruct.) A reject then retracts
  exactly the rows where `source = 'accept'`, the keyword is the card's
  canonical taxon keyword, and no other live non-rejected prediction
  still asserts it — the predicate defined under "A card mutation writes
  every member status before it decides any keyword effect" above, and
  evaluated in Phase B, i.e. after every member's status has been
  written.

  **Provenance is a lattice, and no write may move down it.** With a
  third value the column stops being "set-or-NULL" and
  `COALESCE(excluded.source, photo_keywords.source)` stops being
  sufficient: an accept re-tagging a photo the user had hand-tagged
  would coalesce to `'accept'` and silently downgrade a `'manual'` row,
  which is precisely what #1488's contract forbids and what would then
  make a later reconciling reject strip a user-authored tag. So the
  ordering `manual > accept > NULL` is stated once and enforced at every
  write:

  - `tag_photo`'s upsert becomes a **precedence-max**, not a coalesce —
    the stored value is the greater of the existing and incoming
    provenance. `NULL → accept` and `accept → manual` are the only
    transitions that change a row; `manual → accept`, `accept → NULL`
    and `manual → NULL` are no-ops. #1488's "never downgraded"
    guarantee is the `manual` row of that table, unchanged.
  - **Every path that moves or collapses a `photo_keywords` row folds
    provenance the same way.** `_merge_keyword_into` uses
    `UPDATE OR IGNORE` then `DELETE` (`db.py:13527-13533` on `main`), so
    when a photo already carries *both* the source and destination
    keywords the destination row survives untouched and the source row's
    provenance is discarded. #1488 already pre-folds the one case it
    needs (destination gets `'manual'` if the source row was `'manual'`).
    This design **generalizes that fold to the lattice max over the two
    rows** — needed because the case #1488 does not cover is the reverse
    direction, a `'manual'` destination with an `'accept'` source (must
    stay `'manual'`; already correct) and a `NULL` destination with an
    `'accept'` source (must become `'accept'`, or the accept's own tag
    becomes unretractable the moment a synonym merge runs). The same
    fold applies to keyword rename/curation-merge paths and to any
    future path that rewrites `photo_keywords.keyword_id`. A grep for
    writes to `photo_keywords` is part of the Phase 4 checklist; a path
    that moves a row without folding provenance is a bug, not a gap.
  - **Retraction reads the folded value, and `manual` is never
    retracted.** For each affected photo, a reconciling reject
    enumerates every `photo_keywords` row whose `keywords.taxon_id`
    resolves to the card's taxon (the same `taxa.inat_id` → `taxa.id`
    translation §4 precedence-1 uses) and strips those whose folded
    value is `source = 'accept'`. The enumeration — rather than
    "strip the resolved-once card keyword" — is what makes §4 case 2's
    residual synonym rows retractable: two accepts that happened to
    reuse different taxon-matched keywords on different member photos
    are both accept-owned, and both come out on a subsequent Reject
    all. A row that is `'manual'` — including one that became
    `'manual'` by the fold above, i.e. the user's ownership survived a
    merge into an accept-owned row — is left in place and takes the
    disclosure path below, with the wording adjusted to say why ("kept
    'Blue Tit' — you added this keyword yourself"). This is strictly
    stronger than #1488's contract, not in tension with it: #1488
    promises manual stamps are never downgraded; this design
    additionally promises that a manual-stamped association is never
    removed by an automated review action at all.
- **Retrospectively — never strip on a guess.** A `NULL`-source row is
  left in place. The card says so instead of silently disagreeing with
  its own badge: "Rejected — kept the 'Blue Tit' keyword on 3 photos
  (added before Vireo tracked keyword origin)", with a one-click
  "Remove it too" affordance. The user is the only remaining source of
  truth once provenance is gone, so the design asks rather than picks —
  and either way the photo's metadata and the card's badge are never
  quietly in disagreement, which is the property this whole section is
  protecting. The same disclosure covers the "kept because another live
  prediction still asserts it" case ("kept 'Blue Tit' — still predicted
  on 2 photos"), and takes the workspace-naming form above when the
  surviving assertions are in another workspace rather than on another
  photo of this one — the user cannot act on a count of assertions they
  cannot see from here.

Until #1488 lands and Phase 4 starts writing `'accept'`, every row is
`NULL`- or `'manual'`-source and every reconciling reject takes the
disclosure path — correct, just chattier, which is the right failure
direction for a metadata write. **Sequencing:** Phase 4 depends on
**both #1488 and #1489**. On #1488: its column must be merged first; if
Phase 4 would otherwise start first, it adds the column under the
identical name, type and `'manual'` semantics so the two converge rather
than collide, and it must not introduce a second provenance column or a
second spelling of "a person did this". On #1489: Phase 4 carries §4's
mutation-scoped hoist, which needs the call's target set materialized
*before* the keyword is resolved — the order #1489 introduces and `main`
does not (`db.py:17947` vs `18276` on `main`; `18159` vs `18205` on
`predictions-panel-in-browse`). The two dependencies fail differently
and that is worth keeping straight: without #1488 Phase 4 is merely
chattier, while without #1489 it silently fragments synonyms across a
burst. Only the first is safe to ship ahead of its dependency.

**Compare.** `accept_subject_species` swaps its
`lower(trim(species))` equality for the same taxon-key helper. Its
detection-scoped, single-photo semantics stay unchanged.

**Every mutation entry point, enumerated — routed or deliberately
excluded.**

Everything above specifies the *new* path: a card mutation carrying
`card_id`/`node_id`, frozen membership and the full scope tuple. That is
only half a contract. Review already has several other ways to change a
prediction's status, and each one that keeps its own path is a hole:
a caller that selects raw pending rows and POSTs the legacy
per-prediction endpoint mutates *some* of a merged card's members,
leaving a card whose badge says one thing over rows that say another —
the same class of defect as the badge hole the `mixed` state closes,
arriving through the side door. The rule is therefore stated
exhaustively rather than by example: **every path that writes
`prediction_review.status` from a Review surface is listed below, with
its disposition, and a path that is not listed is not allowed to
exist.** Adding one is a design change, not an implementation detail.

A second closure clause governs *how* a routed path calls, not only
whether it is listed: **a routed entry point issues one card mutation
per card, and never decomposes a card into per-member requests.** A
client loop that POSTs each member separately would satisfy the table
and still break the two-phase rule above, because every request would
run its own Phase B against a state the next request changes — the
"Reject all" keyword leak, reassembled on the client out of individually
correct calls. That is why the toolbar row below iterates *cards* and
issues one mutation each rather than iterating members, and why the
keyboard handler targets a card rather than a row; the batching is a
correctness requirement, not a round-trip optimization.

A third clause fixes *which* cards a bulk or targeted action touches.
**The actionable set is derived from status, never from what happens to
be rendered.** §2's aggregate is a total function on member statuses, so
"actionable" needs no separate notion — it is one predicate over that
table, stated once here and referenced by every caller below:

> `getActionableCards()` = the cards `getVisibleItems()` returns whose
> aggregate status is `pending` or `mixed`. Cards whose aggregate is
> unanimously `accepted` or unanimously `rejected` are terminal and are
> **not** members.

Iterating the visible list instead is a live bug, not a hypothetical:
the default `all` tab applies no status predicate
(`review.html:1298-1300`), so `getVisibleItems()` returns accepted and
rejected cards alongside pending and mixed ones. A toolbar loop over
that list — visible whenever *any* card is actionable — would POST an
accept for every terminal card too, silently reversing every prior
rejection in the view; a keyboard shortcut that grabs `[0]` would fire
at whatever card the current sort happened to put first, terminal or
not. The three callers therefore bind to the set, not to the list:
**Accept All** iterates it, `renderButtons` **counts** it, and `A` / `S`
target its **first element** (first in the grid's own filtered+sorted
order, which is what the toolbar hint promises). A card's own "Accept
all" / "Reject all" buttons are already per-card and per-status, and if
a toolbar **Reject All** is ever added it binds to this same set — the
one definition is the extension point.

The old handler's `status === 'pending'` filter is not a workable
substitute for two independent reasons, and both matter: it is a *row*
predicate where the target is a card, and even read as a card predicate
it omits `mixed`, which §2 makes actionable and gives both reconciling
actions. A shortcut that skips mixed cards is the worse half — those are
exactly the cards holding two contradictory decisions, and the keyboard
is how the user gets through a review queue.

Because Accept All can act on `mixed` cards, it can override prior
rejections. That is the reconciliation §2 specifies, not a surprise, but
it is destructive, so the run's summary names it rather than reporting a
flat count: "Accepted 13 cards — 2 were mixed; 3 prior rejections were
overridden". The number in the button and the number in the summary are
both counts of cards the click acted on.

*Routed through card mutations (Phase 5):*

| entry point | code today | what changes |
| --- | --- | --- |
| Per-card Accept / Reject | `acceptPrediction` / `rejectPrediction` (`review.html:1576`, `1591`) | POST the card mutation with the displayed `card_id` (or `node_id` under a filter), the frozen `observed` map (member ids → the statuses this render displayed), and the scope tuple. This is the happy path §2/§3 specify. |
| Toolbar **Accept All** | `acceptAllPending` (`review.html:1618-1633`) — filters `predictions` to raw `status === 'pending'` rows and POSTs `/api/predictions/<id>/accept` per row | Iterates `getActionableCards()` — cards, not raw rows, and actionable cards, not every displayed one — and issues one card mutation per card with that card's own frozen membership and the same scope tuple. Iterating `getVisibleItems()` directly would submit mutations for terminal cards on the `all` tab (see the actionable-set clause above). Without this, "Accept All" on a `{pending, rejected}` card accepts the pending member and leaves the rejected one rejected — i.e. it *creates* a `mixed` card out of the click that was supposed to resolve it, and it does so bypassing the reconciling-reject retraction rule above. A card whose mutation comes back **409 `card_changed`** (§2) is skipped, not retried and not fatal to the run: the loop continues, that card keeps its own inline notice, and the run's summary reports the worse outcome — "Accepted 12 of 13 cards — 1 card changed and was skipped; nothing was changed on it" — rather than a plain completion. |
| Toolbar button label and visibility | `renderButtons` (`review.html:1258-1273`) — counts raw pending rows for "Accept All (N)" and hides the button when that count is 0 | Counts `getActionableCards()` — the same set Accept All iterates, so the number on the button is by construction the number of cards the click acts on. Counting rows would promise "Accept All (7)" over 4 cards, and — worse — would *hide* the button on a view whose only cards are `{accepted, rejected}` mixed, which are exactly the cards that need reconciling. Same rule as the badge: the number the user reads must be the number of things the click acts on. |
| Keyboard `A` / `S` | keydown handler (`review.html:1638-1655`), currently `acceptPrediction(pending[0].id)` over `getVisibleItems()` rows filtered to `status === 'pending'` | Targets `getActionableCards()[0]` and issues that card's mutation. Two changes, not one: the target becomes a card rather than a row id (leaving it on row ids would silently half-accept the first card), **and** the `pending`-only filter is replaced by the shared actionable predicate, so `A` / `S` reach `mixed` cards instead of skipping them. Swapping only the id would leave the keys inert on a queue whose first — or only — actionable card is mixed; dropping the filter entirely would let them mutate a terminal card. |

*Deliberately excluded, with the reason:*

| entry point | code | why it stays on the legacy per-prediction path |
| --- | --- | --- |
| Alternative pick | `acceptAlternative` (`review.html:1603-1616`) | `alternative` rows are attached to a parent row and are **never card members** (§2 status table; `review.html:1491`). Picking an alternative is a within-node re-rank of one detection's candidates, not a decision about the card's taxon — and it may well change the row's taxon, at which point it belongs to a *different* card. It keeps `/api/predictions/<id>/accept`. The consequence is admitted, not hidden: this can leave the surrounding card `mixed`, which is §2 "Reachability" case (b), and the card's own Accept all / Reject all is the exit. |
| Burst group apply | `/api/predictions/group/apply` (`review.html:3209`, `app.py:15968`) | Writes photo **flags** (pick/reject) and a species keyword for a burst; it is not a prediction-review status write, so it has no card semantics to honour. Unchanged. |
| Context-menu rating / flag | `setReviewRating` / `setReviewFlag` | Photo metadata, not prediction status. Unchanged. |
| Compare's accept-subject | `accept_subject_species` (`app.py:15734`) | No card exists on Compare. It gets §3's taxon-key broadening and §4's **row-keyed** canonicalization (which is why §4 is keyed on the row's taxon rather than on a card — see §3 "Ordering constraint"), but not card mutations. |
| ID-conflicts resolution | `id_conflicts.html:1566-1567` → `/accept-subject`, `/replace-keywords` | A separate per-prediction conflict surface outside Review, with its own semantics. Unchanged; it is another `mixed`-reachability producer, same as Compare. |
| Mark-reviewed | `/api/predictions/<id>/reviewed` (`app.py:15634`) | Sets the reviewed flag; does not flip accept/reject status. Unchanged. |
| Pipeline group apply | `/api/pipeline/group/apply` (`pipeline_review.html`, `pipeline_rapid_review.html`) | Flags-only and never touches `predictions`, as those templates' own comments state. Unchanged. |

*Server-side:* `/api/predictions/<id>/accept` and `/reject`
(`app.py:15699`, `15779`) are **retained**, unchanged, as the primitive
the excluded callers use and as the rollout fallback for old client
payloads (§2's `group_id`/prediction-id dedup fallback). They stay
scope-less and stay a pure status flip — in particular the
reconciling-reject retraction specified above is a property of the
**card** mutation, not of the legacy endpoint, because the legacy
endpoint has no card and therefore no canonical taxon keyword to
retract. Review's card UI simply stops calling them.

**Ordering constraint (§3 depends on §4's canonicalized keyword write).**
Both the Review sibling pass ("Sibling pass, taxon-matched, per photo,
within the resolved scope" above) and Compare's broadened
`accept_subject_species` iterate over agreeing rows and route each
through the existing `_accept_for_photo` / `accept_prediction`
primitives. Those primitives write a keyword whose name comes from
*each row's own* `species` value. If §3's taxon-match broadening
landed before §4's canonicalized keyword write, accepting a Blue-Tit
+ Eurasian-Blue-Tit merged card would write *both* synonym keywords
to the photo — exactly the fragmentation this design is intended to
prevent. §4 therefore has to be live before §3's cross-variant accept
fires, so every sibling accept in the loop resolves to the same
canonical keyword regardless of which row's `species` string it
carries. The Implementation-phases section below sequences the two
accordingly (keyword canonicalization lands as Phase 4; the
cross-model accept/reject and Compare broadening land as Phase 5,
after canonicalization is in place).

This ordering is only sufficient because §4's canonicalization is
specified **inside the per-row accept primitive, keyed on the accepted
row's own taxon** — see §4, "Keyword written on accept". Compare's
`accept_subject_species` has no card to key on, so a card-keyed rule
would have left the Compare half of Phase 5 fragmenting synonyms even
with Phase 4 shipped first. Row-keyed canonicalization covers both
loops, and covers the first-ever accept of a taxon (where no keyword
exists yet to reuse) because precedence 2 also resolves through the
taxon rather than through the row's raw label. Neither loop therefore
needs to be restructured to "tag once and only update sibling
statuses" — the write is idempotent per taxon on its own.

### 4. Display name and keyword canonicalization

**Card display name:** the resolved taxon's preferred common name from the
taxonomy ("Blue Tit"), with the raw per-model labels visible on the model
chips ("iNat21: Blue Tit · BioCLIP-2.5: Eurasian Blue Tit"). Unresolved
(`name:`-keyed) cards display the raw label as today.

**Keyword written on accept** — precedence. The precedence below is
applied **inside `_accept_for_photo` / `accept_prediction`, keyed on
the taxon of the row being accepted** — *not* keyed on the calling card.
The key comes from §1's per-label resolution, run once over the row set
the call is already iterating (Review: the card's members plus the
sibling scan's rows; Compare: the agreeing detection rows on the photo)
and then looked up by each row's folded label. Resolving row-by-row
would reintroduce §1's divergence at the write surface, which is where
it does the most damage: two rows of one label whose stored
`scientific_name` differs would canonicalize to two different keywords
and write both onto the same photo — the exact duplicate-synonym bug
this section exists to prevent. That placement is load-bearing rather
than incidental:

- It is the only level at which both call sites are covered. Review's
  sibling pass has a card; **Compare's `accept_subject_species` does
  not** — it walks agreeing detection rows on one photo with no card
  in scope. A card-keyed rule would leave the Compare loop writing
  one keyword per row's own `species`, so accepting a "Blue Tit" +
  "Eurasian Blue Tit" agreement in Compare would still tag both
  synonyms. Row-keyed canonicalization fixes both loops with one
  change, and leaves Compare's detection-scoped semantics untouched.
- It makes the property *per row*, so it holds no matter how many
  times the loop runs or in what order: every agreeing row resolves
  through the same taxon to the same keyword — guaranteed rather than
  hoped for, since the resolution is per label over the loop's own row
  set — so the loop is idempotent on the keyword set by construction. Neither loop needs a
  "tag once, then only flip sibling statuses" special case — which
  would otherwise be a second, separately-testable code path in each
  caller.
- Both rows in an agreeing pair resolve to the same taxon *by
  definition* (that is what made them siblings), so row-keyed and
  card-keyed agree whenever a card exists. Row-keyed is strictly
  more general.

Precedence, then:

1. If a keyword already exists whose `keywords.taxon_id` matches the
   accepted row's taxon, reuse it — *its* name is what gets written.
   This keeps new accepts consistent with photos already tagged (no
   "Blue Tit" keyword appearing alongside an established "Eurasian
   Blue Tit" keyword), because keywords are global across workspaces
   and feed XMP sidecars on disk.

   **ID-space translation is required before this comparison.** The
   row's `taxon_key` carries the *iNaturalist* ID: `Taxonomy.lookup`
   populates its entries from the taxonomy payload's `taxon_id` field,
   which is the iNat identifier (`taxonomy.py:1217`). But
   `keywords.taxon_id` is a foreign key to the local
   autoincrement `taxa.id` (`db.py:724`), and the iNat value is stored
   separately as `taxa.inat_id` (`db.py:706-708`). Comparing the card's
   iNat id directly against `keywords.taxon_id` will therefore usually
   miss an established synonym keyword — and, worse, an accidental
   numeric collision (`taxa.id == some_other_taxon.inat_id`) would
   silently reuse an *unrelated* keyword. Precedence-1 lookup therefore
   resolves through `taxa.inat_id` first:
   `SELECT id FROM taxa WHERE inat_id = ?` with the row's iNat id,
   and only then a taxon-scoped lookup against `keywords` with that
   local `taxa.id`. If the taxa row is absent (an unknown iNat
   id, or a payload that predates the taxa refresh), precedence-1
   yields no match and step 2 runs. For `name:`-keyed rows (no
   resolved taxon), precedence-1 is skipped entirely.

   **Prefer a taxon-matched keyword already attached to the accepted
   row's photo, then fall back deterministically.** `keywords.taxon_id`
   is indexed but *not* unique (`db.py` schema; the design also
   explicitly admits taxon-duplicate keywords as a residual — see the
   "No retroactive rename" note below and the Keywords-page follow-up).
   Two synonym keywords ("Blue Tit" and "Eurasian Blue Tit") can
   therefore both point at the same local `taxa.id`, and an unordered
   `SELECT id FROM keywords WHERE taxon_id = ?` may return either — so
   an accept against a photo already carrying "Eurasian Blue Tit" could
   pick "Blue Tit" from the same taxon bucket, attach it too, and
   produce the very fragmentation this precedence exists to prevent.
   The lookup is therefore two-step, in this exact order:

   1. **Photo-attached first.** Restrict candidates to keywords already
      attached to *this accepted row's photo* via `photo_keywords`:
      `SELECT k.id FROM keywords k JOIN photo_keywords pk ON
      pk.keyword_id = k.id WHERE k.taxon_id = ? AND pk.photo_id = ?`.
      If any row survives, reuse it — its name is what gets written,
      and by construction the accept is a status-only no-op on the
      keyword. When multiple taxon-matched keywords are *all* already
      attached (a pre-existing duplicate the follow-up merge tool
      hasn't cleaned up yet), pick the deterministic fallback ordering
      below among them so the choice is stable across accepts on the
      same photo.
   2. **Deterministic global fallback.** Only if step (i) finds
      nothing, widen to `SELECT id FROM keywords WHERE taxon_id = ?`
      **`ORDER BY id ASC LIMIT 1`** — the lowest-id keyword pointing at
      the taxon. `keywords.id` is monotonically assigned by
      `add_keyword` on first sight, so the oldest keyword for the taxon
      wins; this is stable across accepts, across workspaces (keywords
      are global), and independent of insertion order in the taxon
      bucket. It matches what a first-accept would have converged on
      before duplicates accumulated, so accepts on photos with no prior
      tag still converge on the same keyword as accepts on photos that
      already carry one.

   Both steps run under the ID-space translation above (through
   `taxa.inat_id` first, then by local `taxa.id`). For `name:`-keyed
   rows the whole two-step block is skipped exactly as before —
   `name:` rows have no `taxa.id` to key on.

   **Resolved once per mutation, not once per photo — one accept writes
   at most one keyword string.** The two steps above are stated
   per accepted row, and run that way they are not stable across any
   mutation that spans photos — a grouped accept or a card:
   a member photo already carrying "Eurasian Blue Tit" takes step (i)
   and reuses it, while an untagged member photo falls to step (ii) and
   takes the globally oldest "Blue Tit". One click would then attach two
   spellings of one taxon across one card — fragmentation created by the
   mechanism that exists to prevent it, and a card note naming either
   string would be false about the other half. The resolution is
   therefore hoisted to the **mutation** and evaluated once over the
   union of member photos:

   1. **Some member photo already carries a taxon-matched keyword, and
      they all carry the same one.** That keyword is the card's keyword.
      Every already-tagged photo is a status-only no-op and every
      untagged photo converges on the spelling the card already uses.
   2. **Member photos carry two or more *different* taxon-matched
      keywords** (pre-existing fragmentation the merge follow-up hasn't
      cleaned up). No single string can be written everywhere without
      adding a synonym to a photo that already has one, so the card does
      the strictly non-destructive thing: already-tagged photos keep
      what they have, untouched, and the card's keyword — the one
      *written*, to untagged member photos — is the lowest-`id` keyword
      **among those already present on the card**, not the global
      lowest-id. Restricting to the card's own spellings is what keeps
      the accept from introducing a *third* spelling into a photo set
      that already disagrees.
   3. **No member photo carries any taxon-matched keyword.** Step (ii)'s
      deterministic global fallback, once, for the whole card.

   The resulting invariant is exact and is what the card can honestly
   promise: **a card accept writes at most one keyword string; it may
   leave other synonyms already on member photos in place, and it never
   adds a spelling that no member photo had.**

   **The unit the union is taken over is the call's resolved target set,
   and a card is not the first thing to have more than one photo in
   it.** An earlier draft said the hoist could wait for Phase 5 because
   "all existing callers are single-photo, so their union is one photo".
   That is true of exactly one of them. Compare's
   `accept_subject_species` really is single-photo — it passes
   `photo_ids=[target["photo_id"]]` on every delegated call precisely so
   that accepting a subject cannot tag the rest of a burst
   (`db.py:18360-18364`). The legacy per-prediction accept is not:
   `accept_prediction` on a grouped row expands to every in-scope group
   member and calls `_accept_for_photo` once per member photo
   (`db.py:18276-18290`). Run the per-row precedence inside that loop and
   one *existing* click fragments synonyms across a burst — member photo
   1 already carrying "Eurasian Blue Tit" takes step (i), member photo 2
   falls to step (ii) and takes the globally oldest "Blue Tit" — which
   is a regression introduced by Phase 4 and not repaired until Phase 5.
   Today's code does not have this bug only because it resolves one
   `add_keyword(species)` for the whole call and writes that string
   everywhere; Phase 4 is what makes the resolution photo-dependent, so
   Phase 4 is what owes the hoist.

   **The hoist is therefore Phase 4, over the grouped accept's target
   set.** The rule is unchanged in substance — cases 1–3 above, evaluated
   once over the union of the photos the call will actually tag, which is
   `targets` after both scope limits and the undecided-only narrowing
   have settled, *not* the raw group. Phase 5 then reuses it verbatim for
   a card, whose union spans photos from more than one group; that is a
   wider union, not a new mechanism, which is why Phase 5's fixtures
   below still carry the multi-photo assertions.

   **This adds a dependency on PR #1489, for a structural reason and not
   a stylistic one.** On `main` the union does not exist at the point the
   keyword is chosen: `add_keyword` runs at `db.py:17947` and the group
   is not expanded until `db.py:18276`, so there is literally no target
   photo set to consult when the keyword is resolved. #1489 already
   reorders exactly this — it materializes the in-scope target list
   first (`db.py:18159` on `predictions-panel-in-browse`) and resolves
   the keyword after (`db.py:18205`) — which is the shape the hoist
   needs. Phase 4 therefore lands after #1489, or repeats that
   reordering itself; it must not do it a second, divergent way. #1489
   also narrows what the union contains: its undecided-only expansion
   drops already-decided members, and its `prediction_ids` limit is
   stricter than `photo_ids`. Stating the union as "the rows this call
   will tag" rather than "the group's photos" is what makes the rule
   correct under either merge order.
2. Otherwise, create/use **the taxon's preferred common name** — again
   the taxon's, not the row's raw `species`. This is what makes the
   *first* accept of an agreeing pair safe: before any keyword exists
   for *Cyanistes caeruleus*, precedence 1 misses for both rows, and
   only a taxon-derived name makes the "Blue Tit" row and the
   "Eurasian Blue Tit" row converge on one keyword. Falling back to
   the row's own `species` here would reintroduce the exact
   fragmentation precedence 1 prevents, one accept earlier.
   `name:`-keyed rows (unresolved) keep writing their raw label, as
   today.

**Transparency requirement (CORE_PHILOSOPHY):** when the keyword string that
accept will write differs from the card's display name, the card says so —
e.g. a subdued `tags as "Eurasian Blue Tit"` note under the title. The card
must answer "what will accepting this do", not just "what species is this".

The note is truthful because the rule above made it possible to be
truthful: there is exactly one written string per card accept, so
`tags as "…"` has a single well-defined value rather than a
representative one. Case 2 above is the one where a single string is
still not the whole story — some member photos keep a different synonym
— and the note says both halves rather than naming the winner and
staying quiet:

```text
tags as "Blue Tit" — 2 photos keep "Eurasian Blue Tit"
```

Naming only the written string there would answer "what gets written"
while the user reads it as "what this card's photos will be tagged", and
those differ; that is the substitution the no-black-boxes rule forbids.
Rendering per-photo outcomes was the other option and is rejected: it
scales with card size, it puts a table under a grid card, and it says
nothing case 2's one extra clause does not, since the retained spellings
are a set of at most a handful of strings, not a per-photo fact. The
retained-synonym clause is also the in-context surface for the
Keywords-page merge follow-up below — it is exactly the state that tool
cleans up, named at the moment the user is looking at it.

No retroactive rename of existing keywords. Follow-up (out of scope):
surface taxon-duplicate keywords ("2 keywords resolve to *Cyanistes
caeruleus*") on the Keywords page with a one-click merge.

## Edge cases

- **Unresolvable labels** (custom label files, "Duck sp."): fold-string key;
  merge only on identical strings — behavior unchanged from today.
- **Taxonomy not loaded / offline:** every key degrades to `name:`;
  Review behaves exactly as it does today. Deterministic, no errors.
- **Mixed-provenance `scientific_name` within one label** (a burst
  half-composed of cached rows written before `taxonomy.json` was
  installed): the label resolves once, from the union of its rows'
  scientific names, so the stale-`NULL` rows inherit the resolved key
  instead of splitting off as `name:` (§1, "One key per label"). Two
  scientific names that resolve to *different* taxa are treated as a
  conflict, not a vote: the whole ladder is abandoned and the label's
  key is `('name', L)` — not the next rung down, which would re-pick a
  single taxon from the label string and merge the rows anyway (§1,
  "Conflicts fall to the name key, not to the next rung").
- **Alternatives** (`status="alternative"` rows): not review cards; excluded
  from card building and untouched by sibling resolution. They are
  therefore never card *members* either, which is what keeps the card
  status aggregate a total function over `{pending, accepted, rejected}`
  (§2, "Client changes").
- **Mixed member outcomes** (one member accepted, another rejected): a
  real, pre-existing state — separately decided rows merge into one card
  the first time the `all` tab builds it. The card renders as `mixed`
  with the per-status member counts and both "Accept all" / "Reject all"
  actions; it is never reported as resolved. §2 "Client changes" has the
  full status table and §3 step 3 the reconciling mutation.
- **Workspace scoping:** all card building and sibling resolution joins
  through `prediction_review` for the active workspace, as accept does now.
- **`api_lookup` latency:** never on the request path. The Review GET
  reads cached results only; unresolved labels degrade to `name:` fallback
  until the background resolver populates the cache. Offline/firewalled
  installs therefore never wait on the network to render Review, and a
  transient outage does not degrade page latency at all.
- **Transitive card components:** the accept path always operates on the
  card's pre-computed union of photos, so a chain (A: 1-2, B: 2-3, C: 3-4)
  resolves in one click. See §3, "Enumerate photos from the component".
- **Two same-taxon groups from one model** (re-runs under different
  fingerprints, *or* two passes under the same model and the same
  fingerprint — classifying this week's import the same way as last
  week's is the ordinary case): they merge into one card if their photos
  overlap — which is the correct de-duplication, and the fingerprint
  filter still separates them when the user asks. (This is a cross-*run*
  merge — the run keys are disjoint — so §2's "Within-run subject
  partition is authoritative on the edge" rule permits it, unlike the
  same-run case below. The same-configuration half of this case only
  works because run identity is a persisted per-pass token; under a
  `(classifier_model, labels_fingerprint)` proxy those two passes would
  read as one run and never merge.)
- **Distinct same-taxon subjects on one photo.** One classify run
  can — and routinely does — produce two nodes on the same photo that
  both predict the same taxon: a real Blue Tit box in one `group_id`
  and a false-positive Blue Tit box on background clutter in another
  `group_id` (or one grouped and one singleton). Similarity grouping
  ran over that photo's detections at classify time and *chose* the
  split, based on image evidence the Review layer no longer has. §2's
  edge rule ("Within-run subject partition is authoritative on the
  edge") preserves the choice: no `(taxon_key, overlapping photos)`
  edge is drawn between two nodes whose run keys intersect — the
  grouped/singleton pair included, which is why the run token is
  persisted per row and not only inside `group_id` — so the two subjects
  render as two separate cards and one accept-or-reject decision does
  not silently resolve the other. The **cross-run** case — a second
  classifier also produces its own real-vs-false split for the same
  photo — is genuinely ambiguous from `(photo, taxon)` alone: neither
  side's `group_id`s carry information about the other side's subject
  partition, so pairing them up on a single photo is a guess.
  Ambiguous photos are dropped from the edge's overlap set (either
  side's multiplicity suppresses them), and the cross-run merge either
  proceeds through some *other* unambiguous frame in the same burst
  (the far commoner case, since bursts hold many frames) or fails to
  merge and the two cross-run cards remain separate — the more
  conservative failure mode. Keyword *metadata* is unaffected either
  way: a photo carries at most one keyword per taxon regardless of how
  many same-taxon detections it holds, and the liveness clause of §3
  ("A card mutation writes every member status before it decides any
  keyword effect") keeps the tag on the photo as long as any
  same-taxon card there is still `pending` or `accepted`. The
  granularity difference is intentional: *review status* is per-card
  (each subject decided independently), *keyword* is per-photo (one
  Blue Tit tag either way).

## Alternatives considered

- **Hard-coded alias map** ("Eurasian Blue Tit" → "Blue Tit"): fixes one
  species, recurs for every regional name, and encodes taxonomy opinions in
  code. Rejected.
- **Normalize labels at classify/store time** (rewrite `species` to the
  preferred common name before insert): destroys fidelity of what the model
  actually said (transparency violation), breaks the
  `labels_fingerprint`-aware skip gate (`get_existing_prediction_photo_ids`
  matches stored rows against label-set contents), and risks colliding with
  the `UNIQUE(detection_id, classifier_model, labels_fingerprint, species)`
  constraint on re-runs. Rejected.
- **Client-side merging:** the client lacks the taxonomy, and the accept
  path needs the same key server-side regardless. Rejected.
- **Merge only on identical photo membership** (the naive rule): silently
  fails whenever the models' groups differ by one frame, which is common;
  the feature would appear broken. Rejected in favor of overlap components.
- **A fifth node-key component to force one taxon per node.** Two shapes,
  both rejected. (a) *The resolved taxon key.* It is not a function of
  immutable row columns — it moves when the taxonomy cache moves — so it
  would undo the one property §2 "Node identity" exists to establish, and
  every `node_id` the client holds would stop decoding across exactly the
  background-resolver transition the anchor design was built to survive
  ("Why the anchor alone"). (b) *The stored `scientific_name`.* That one
  *is* immutable per row, and it is still wrong: it splits the ordinary
  cached-plus-new burst — half the rows written before `taxonomy.json`
  landed, half after — into two nodes, and those shards can never
  re-merge, because every `predictions` row carries exactly one
  `photo_id` (`db.py:865-883`) and a burst holds one row per photo, so
  two shards of one burst are photo-disjoint and the same-taxon *plus*
  overlapping-photos edge has nothing to join them on. That is the
  over-splitting regression §2 rejects, arriving on the common path.
  Canonicalizing the key (§1) instead of partitioning the node keeps
  identity immutable and the burst whole.
- **Resolve the taxon key at write time and freeze it** (an additive
  `predictions.taxon_id` stamped by `add_prediction`): it does not solve
  the divergence, it makes it permanent. The whole problem is that what a
  run could resolve depends on when it ran; freezing the answer per row
  pins a row written while `taxonomy.json` was absent to `name:` forever,
  where a read-time key heals as soon as the taxonomy or the lookup cache
  improves. It also needs the schema change §1 defers and cannot apply
  retroactively to existing rows without a backfill, against the "works
  on existing prediction rows with no destructive migration" goal.
  Rejected.

## Implementation phases

Each phase lands as its own PR and is independently useful.

0. **Per-pass run token: collision-resistant `group_id` + a persisted
   run identity** (prerequisite of Phase 3; see §2 "Node identity" and
   "Run key"). Mint `run_token = secrets.token_hex(16)` once per
   `_store_grouped_predictions` call and change the ID template from
   `f"g{job_id[-6:]}-{group_count:04d}"` to
   `f"g{run_token}-{group_count:04d}"`. Neither the
   truncated nor the *full* `job_id` is acceptable: `JobRunner` resets
   `_enqueue_counter` to `0` on every process start (`jobs.py:112,
   687-689`), so `job_id` is unique only within a process and a
   restart plus a backward clock adjustment can re-mint one. Add
   `predictions.run_token TEXT` (`ALTER TABLE`, nullable, `db_meta`
   marker — not `user_version`), stamp it on every row the pass stores,
   and refresh it on the `_existing` reuse branch alongside the existing
   group-metadata update/clear so token and partition always come from
   the same pass. Tests:
   the same pass's group IDs remain distinct; two independently minted
   jobs' group IDs are always distinct across every combination of
   `classifier_model` × `labels_fingerprint`; a **restart-collision
   fixture** — two jobs of the same `job_type` given the *same*
   `job_id` (simulating a `_enqueue_counter` reset with a repeated
   wall-clock millisecond) still mint disjoint `group_id`s, which the
   full-`job_id` template would have failed; a **run-token pairing
   fixture** — every row a single pass stores shares one token, that
   token is the prefix of every `group_id` the pass minted, and two
   passes with the *same* `(classifier_model, labels_fingerprint)` get
   different tokens; a **regroup fixture** — a row grouped by pass 1 and
   re-seen (regrouped, or ungrouped via `clear_prediction_group_info`)
   by pass 2 ends up carrying pass 2's token, not pass 1's; a
   **legacy-namespace fixture** — a pre-migration row's run key is
   `legacy:{model}:{fp}` and never equals any stamped token; existing
   consumers of
   `group_id` (`add_prediction`, `prediction_review.group_id`,
   `_folded_species_key`, the review-endpoint dedup path) accept the
   longer string unchanged (opaque). One additive nullable column, no
   backfill, no read-side change; safe to land ahead of the merge
   feature.

   Same phase, same file, same "stop generating the mess" character:
   **backfill a `NULL` `scientific_name` on the reuse path.** The
   `_existing` branch of `_store_pending_detection_prediction`
   (`classify_job.py:2061-2099`) currently returns after updating group
   metadata, so a row inserted when the taxonomy could not resolve its
   label keeps `scientific_name = NULL` through every later run that
   *can* resolve it. Add one `UPDATE` there, guarded two ways: only when
   the current run has a hierarchy for the label, and only when the
   stored value **is `NULL`**. Never overwrite a non-`NULL` value — a
   taxonomy revision that remaps a name is a decision, not a repair, and
   silently rewriting stored classifier output would be the transparency
   violation the "normalize labels at store time" alternative was
   rejected for. This does not replace §1's per-label resolution (rows no
   run revisits are never repaired, and the read path must be correct on
   its own); it shrinks the divergent population instead of letting it
   accumulate. Test: a row stored with `NULL` under a taxonomy-less run,
   re-seen by a run whose taxonomy resolves the label, ends up populated;
   a row whose stored scientific name disagrees with the current
   taxonomy is left untouched.
1. **Taxon key helper** (`taxonomy.py`) + unit tests: scientific-name hit,
   common-name hit, normalized hit, alternate-name via **pre-seeded lookup
   cache** (no live HTTP in the test path), unresolvable fallback,
   NULL handling, rank separation. Also: `cached_api_lookup` returns
   `None` on miss and never opens a socket. Per-label resolution (§1,
   "One key per label") gets its own unit tests, since the helper now
   takes a label and a *set*: a label whose rows carry one scientific
   name plus several `NULL`s resolves off the populated one; two
   scientific names resolving to the same `taxon_id` resolve to it; two
   resolving to **different** `taxon_id`s **abandon the ladder entirely**
   and return `('name', L)` on the canonical folded spelling — rungs 2
   and 3 are *not* consulted, asserted under a label whose common-name
   lookup **does** resolve and with a populated alternate-name cache
   entry, so an implementation that merely skips rung 1 returns some
   `taxon:` key here and fails (§1, "Conflicts fall to the name key, not
   to the next rung"); the canonical
   spelling fed to `Taxonomy.lookup` is the same for `Say's Phoebe`,
   `Say’s Phoebe` and `Say's phoebe`; and the function is
   order-independent — shuffling the input rows cannot change the key.
2. **Background taxonomy resolver** (`jobs.py::resolve_taxonomy_labels`):
   drains the enqueued-labels queue, persists hits and misses (with
   exponential-then-daily retry on misses), single-flight. Tests use a
   stubbed `api_lookup` and assert (a) hits populate the cache, (b) misses
   are recorded with a retry timestamp, (c) `/api/predictions` never calls
   `api_lookup` directly.
3. **Server-side cards, payload only**: `taxon_key`/`card_id`/`node_id`/
   `display_name` on every `/api/predictions` row, the merge graph
   behind them, and the scope-carrying card endpoint. `review.html` is
   **not** switched over in this phase: it keeps dedupping on `group_id`
   exactly as today, so the phase is additive and invisible.

   **Why the renderer waits for Phase 5.** Merged rendering and the
   card-aware mutation have to ship together. If Phase 3 started
   collapsing two models' rows into one card while accept/reject still
   went through today's endpoint, a click would accept only the
   representative row's own same-model group; the other model's rows
   would sit pending *inside* a card that now renders as resolved, and
   clicking again would keep hitting the same representative — the
   motivating bug made worse, not better, for the length of two phases.
   The reverse order is not available either: §3's cross-model accept
   must land after §4's keyword canonicalization or the sibling loop
   fragments synonyms ("Ordering constraint"). So the split is by
   *layer*, not by surface: the server learns cards in Phase 3, and the
   client starts using them in Phase 5, in the same PR as the mutation
   that can resolve them. Consequently every fixture below that asserts
   **rendering** (merged cards, aggregate/`mixed` status, the card
   endpoint's use by the modal) is written in Phase 3 but runs against
   Phase 5; the API-level fixtures — which `card_id` a row gets, which
   nodes merge — run in Phase 3.

   API tests: the Blue Tit fixture (two
   models, same taxon, 8-vs-7 overlap) yields one `card_id`; different taxa
   don't merge; model filter yields per-model cards; **transitive overlap
   fixture** (three groups A/B/C forming an A-B-C chain, same taxon) yields
   one `card_id` covering the full union; **group-id-uniqueness
   fixture** (two classify jobs with the *same* `classifier_model` and
   `labels_fingerprint`, run back-to-back so their timestamps land in
   the same second and their per-job group counters both start at 1)
   mint distinct `group_id`s under the Phase 0 write path — 128 bits
   of entropy per pass makes them different by construction — so
   their `(classifier_model, labels_fingerprint, group_id,
   species_key)` nodes are distinct and their disjoint bursts stay as
   two separate cards; a **cross-pass same-configuration fixture** —
   two passes sharing a `classifier_model` and `labels_fingerprint`
   whose same-taxon groups *overlap* on photos: assert they merge into
   **one** card, which the `(classifier_model, labels_fingerprint)`
   proxy would have suppressed as if the two passes were one run; its
   mirror, a **within-pass split fixture** — one pass producing two
   same-taxon nodes on the same photos, one grouped and one singleton
   (so the split is not visible in `group_id` alone), stays **two**
   cards because both rows carry that pass's `run_token`; a
   **legacy-suppression fixture** — two pre-Phase-0 rows with the same
   `(classifier_model, labels_fingerprint)` and no `run_token` keep
   today's suppression through the `legacy:` namespace; **legacy-collision
   split fixture** — the same fixture built with legacy pre-Phase-0
   rows (same short suffix + counter, colliding `group_id`) where the
   two bursts are of *different* species resolves as two cards,
   because the species key splits the bucket into two nodes with no
   edge between them; a **same-species legacy fixture** where the two
   colliding bursts share a species collapses into one node and one
   card — the documented residual, identical to today's
   `group_id`-only client dedup, and closed prospectively by Phase 0;
   a **similarity-refined burst fixture** — the regression that killed
   the capture-time rule: one stored burst produced by
   `refine_groups_by_similarity` (`classify_job.py:2170-2172`) whose
   member capture times are 0s and 7000s apart because the
   non-similar intervening frame went to another subgroup
   (`grouping.py:83-119`). Assert it resolves as **one** node and one
   card. A `W_read`-gap partition with any fixed window splits it; a
   species-key partition cannot, because the burst is unanimous in
   species by construction (`group_reviewable`,
   `classify_job.py:2269-2272`); a **cached-plus-new burst
   fixture** — the regression that killed the
   `predictions.created_at` rule: one burst whose photos are half
   cached rows (written weeks earlier, re-injected into
   `raw_results` by the non-`reclassify` gated path,
   `classify_job.py:1657-1712`, and left untouched by
   `add_prediction`'s `INSERT OR IGNORE`) and half freshly inferred
   rows, all assigned one `group_id` by
   `_store_grouped_predictions`. Assert it resolves as **one** node
   and one card; a **mixed-provenance taxon-key fixture** — the same
   cached-plus-new burst, but now the halves disagree on
   `predictions.scientific_name`: the cached rows carry `NULL` (stored
   under a run with no taxonomy file) and the fresh rows carry
   `Cyanistes caeruleus`, under a label (`Eurasian Blue Tit`) that the
   local taxonomy's common-name index does **not** resolve, so the
   ladder's rung 1 fires for one half and rung 4 for the other. Assert
   every row of the burst gets the **same** `taxon_key`, the burst is
   one node and one card, and the other model's group over the same
   photos — all `NULL` scientific names — merges into it rather than
   rendering separately. A per-row ladder yields `taxon:13094` and
   `name:eurasian blue tit` inside one node and fails on the first
   assertion; a **scientific-name-conflict fixture** — the same burst,
   but the two stored scientific names resolve to *different* taxon
   ids, under a label the common-name index **does** resolve (so rung 2
   would succeed if it were consulted), plus an alternate-label group
   on overlapping photos that resolves to one of the two candidate
   taxa. Assert the conflicting label's key is `name:<folded label>`
   for every one of its rows, that its nodes merge with each other and
   with nothing else, and specifically that the alternate-label group
   renders as a **separate** card. An implementation that merely skips
   rung 1 returns a `taxon:` key here, merges the alternate-label
   group in, and fails the last assertion — this is the regression
   guard for §1, "Conflicts fall to the name key, not to the next
   rung"; a
   **normal-burst-not-split fixture** — an ordinary
   single-job burst of eight rows on eight distinct photos resolves as
   *one* node, not eight (the regression the "photo-connectivity"
   rule would have introduced by putting every single-photo row in its
   own subset); a **case-and-apostrophe-variant burst fixture** — a
   stored burst whose frames spell `Say's Phoebe`, `Say’s Phoebe` and
   `Say's phoebe` resolves as **one** node, because `species_key` is
   the ASCII-folded match key and not the raw string; the residual
   pre-Phase-0 collision surface (two disjoint same-species bursts
   sharing a colliding `group_id`) is documented as unfixable from
   stored rows and closed prospectively by Phase 0; a
   **filter-invariant node identity
   fixture** — the `node_id` the server stamps on a given row is
   byte-identical across GETs made with no filter, with
   `minConfidence` raised above every other row of its bucket, and
   with a status tab active (the case a positional `subset_index` or
   any query-scoped partition would have failed: hiding rows
   renumbers or re-derives the survivor, and the client's `node_id`
   stops resolving);
   **cross-fingerprint hidden-row fixture** (groups A and C at
   fingerprint X, group B at fingerprint Y bridging them by shared
   photos, plus a singleton S with the same taxon on an unrelated
   photo) — with the fingerprint filter set to X, the client dedups
   the filtered rows by node identity and shows A, C, and S as three
   separate cards (the singleton is not collapsed into A/C), and the
   merged-card endpoint is not called; with the filter cleared, A+B+C
   become one card and S stays separate (no photo overlap) as
   expected; **min-confidence hidden-bridge fixture** — groups A and F
   at confidence above the slider, group E at confidence below, E
   shares a photo with both A and F; with `minConfidence` raised above
   E, the fallback triggers (per §2 "Active-filter detection"), A and
   F render as two separate cards, and the merged-card endpoint is
   not called; with `minConfidence = 0`, A+E+F become one card;
   **status-tab hidden-bridge fixture** — a `currentTab = 'pending'`
   view with an already-accepted sibling G on a photo shared between
   pending groups A and F: the fallback triggers, A and F render as
   two separate cards, and G is untouched by any subsequent mutation;
   **visual-clause server-scoping fixture** — an active
   `VireoFilter.getVisual()` visual-search clause whose matched
   photo-id set excludes photo `p*`, on which a same-taxon group V
   sits overlapping pending groups A and F (both otherwise inside
   the matched set): because `_apply_visual_to_rules` folds the clause
   into `rules` before the query (`app.py:15300-15315`), V's rows never
   reach card building, so the **server** emits A and F as two
   components with two distinct `card_id`s, the client stays in
   merged-card mode (the structural test does not fire —
   `predictions.length === allPredictions.length`), and the merged-card
   endpoint **is** reachable and is called with the same `visual`
   payload; a variant that calls it without `visual` returns V and
   fails, which is the regression guard for classifying the clause as
   server-applied. With the visual clause cleared, A+V+F become one
   card;
   **collection-scoped card-endpoint fixture** — a collection filter
   excludes group D (same taxon, photos overlapping visible group A)
   while **no** client-applied predicate is active, so the view stays a
   merged-card view and the client opens the card endpoint: the Review
   GET emits A alone, and
   a card-detail POST whose body carries A's `card_id` and the
   `collection_id` returns A's
   membership only, with D's rows and D's out-of-collection photos
   absent. The same fixture against an endpoint that omits
   `collection_id`/`rules`/`visual` — or that composes
   `/api/predictions/group/<group_id>` — returns D and fails; that is
   the regression guard for corollary 3. A `rules`-only variant
   (universal-filter rule, no collection) asserts the same; a
   **client-collection-divergence fixture** — a stale
   `collectionPhotoIds` set drops a row the server returned
   (`review.html:1126-1131`) with no enumerated predicate active: the
   structural active-filter test fires on
   `predictions.length !== allPredictions.length`, the client falls
   back to per-node cards, and the card endpoint is not called;
   **singleton-collapse-bug fixture** (three singleton predictions of
   the same taxon on three unrelated photos, any filter active) — the
   fallback preserves all three as distinct rows and does not collapse
   them into one; **URL-hostile card-id fixture** — a `name:`-keyed
   card derived from a custom label whose folded form contains `/`,
   `?`, `#`, `|`, and `:` (all of which appear inside the raw
   taxon/member key syntax) round-trips through the JSON-structured
   base64url-encoded id in the card endpoint's **request body**, and the
   server decodes back to the exact `smallest_member_key`; the same id
   also survives a round trip through the URL hash a deep link stores it
   in, which is the surface rule 2's encoding still has to cover now
   that no request path carries the id; a
   **card-payload-size fixture** — a transitive component of 2,000
   member rows opens through the card endpoint successfully, the
   regression guard for serializing membership into a URL (the
   `&rows=…` shape this replaced would exceed the request-line limit and
   fail on exactly the largest cards); a **cache-transition card-id
   fixture** —
   a `name:blue tit` card is emitted with
   `card_id = base64url(JSON([A]))` where `A` is the anchor node
   key, the background resolver then persists `blue tit` →
   *Cyanistes caeruleus* (iNat 13094), and a subsequent mutation
   POST that carries the *original* `card_id` resolves to the
   anchor's rows, computes their current taxon key as `taxon:13094`,
   and finds the anchor's component under that key without returning
   400 — including the sub-case where the same-taxon merge shifted the
   component's smallest-member anchor to a different node than `A` (the
   component still contains `A`, so `A`'s component is still findable
   and `A`'s `card_id` still resolves); a **frozen-membership fixture**
   — the same transition, but a previously-separate BioCLIP-2.5 group
   `B` that already carried `taxon:13094` joins the resolved component
   between the GET and the POST: the POST's `member_prediction_ids` does
   not name `B`'s rows, so it no longer equals `server_members`, and the
   POST is refused **409 `card_changed`** with `joined_prediction_ids`
   naming `B`'s rows — **nothing at all is written**, the clicked card's
   own members stay pending too, and a reload draws the merged card.
   Three negative variants, each a build that would have passed the
   earlier revisions of this rule: (a) a build whose precondition is the
   old one-sided *"intersection equals `member_prediction_ids`"*, given a
   payload that simply **omits one displayed member** — it proceeds and
   half-accepts the card, which is the vacuous-precondition regression;
   (b) a build that excludes `B` and proceeds with `"expanded": 1` — it
   now fails, because growth and under-reporting are the same
   observation and both must refuse; (c) a build where the bound applies
   to the component resolution but **not** to the per-photo sibling scan,
   which re-admits `B`'s row through a shared photo. A fourth,
   positive: a payload naming exactly `server_members` after the merge
   succeeds and writes exactly those rows, pinning the stated residual —
   a forged membership can suppress the 409, and can reach nothing
   outside `component ∩ scope`.
   A **cache-transition split-component fixture** — the same GET as
   the frozen-membership fixture, but between the GET and the POST a
   new classify run adds a conflicting `scientific_name` for exactly
   one of the card's labels `L1` (the label some members carry but not
   others), which §1's per-label conflict rule then keys as
   `name:blue tit` for the whole request; the anchor's alternate label
   `L2` still resolves to `taxon:13094`, so the anchor rebuilds under
   `L2` into a smaller `taxon:13094` component that does *not* include
   the `L1`-only frozen members. The POST returns **409
   `card_changed`** with `departed_prediction_ids` naming exactly the
   `L1`-only rows, `departed_count` matching, and `current_cards`
   carrying the current decomposition; no status flip is written for
   any member, no keyword is touched and no history entry is created
   (Phase A is not entered); and the client-side reload re-renders the
   card as two smaller cards under their new taxon keys. Three negative
   variants: a build that drops the subset check and silently applies
   the shrunken intersection leaves the `L1`-only members `pending` and
   fails (badge-disagrees-with-metadata on the split boundary); a build
   that treats the split as a 400 anchor-gone response fails, since the
   anchor's rows *are* still there and 400 tells the client the click
   was unrecoverable when it was not; and a build that returns the 409
   but renders no card-level notice fails, because a click that is
   refused without explanation is the same black box as one that
   silently narrows. A **scope-departure variant** reaches the identical
   409 with no taxonomy movement at all: a frozen member's status
   changes out of the active status tab (second variant: its photo
   leaves the selected collection) between the GET and the POST, so
   `∩ scope` drops it — the guard for stating the precondition over
   `resolved component ∩ scope` rather than over the component alone. A
   **card-split batch fixture** — toolbar "Accept All" over three
   cards, one of which splits this way: the other two accept fully, the
   split one is left untouched and actionable, and the run reports the
   worse outcome ("Accepted 2 of 3 cards — 1 card changed and was
   skipped") rather than a completed Accept All; a build that aborts the
   whole run on the first 409 fails, and so does one that reports
   "Accept All complete". A **card-split read fixture** — the same
   split against `POST /api/predictions/card` returns **200** with the
   surviving members, `"departed"`, `departed_prediction_ids` and
   `current_cards` populated, and the modal's actions rendered disabled;
   a build that 409s the read fails (a read that refuses shows the user
   nothing while the grid still shows the card), and so does one that
   returns the survivors with no `departed` disclosure. A
   **row-vs-node freeze fixture** — no taxonomy transition at all:
   between the GET and the POST a photo of a *frozen* node joins the
   selected collection (and, in a second variant, a row of that node
   changes into the active status tab), so the rebuilt node legitimately
   contains a row that was never displayed. The mutation must not write
   that row: it appears in `server_members`, the equality check fails,
   and the POST 409s with it in `joined_prediction_ids`. A node-level
   freeze admits it silently (the node id matches) and fails the fixture,
   which is why membership is compared at prediction-id granularity. A
   **frozen-membership tampering fixture** — a POST naming prediction
   ids outside the resolved component or outside the scope tuple is
   refused 409 with those ids in `departed_prediction_ids` and writes
   nothing; a build that instead intersects them away and proceeds fails,
   because it would silently narrow the click on any honest client whose
   card really did split. A **short-payload fixture** — a POST naming a
   strict subset of the displayed members is refused 409 and writes
   nothing; a build that accepts the subset fails. Together these two
   pin that `member_prediction_ids` is a precondition, never a selector,
   in both directions. A **stale-status fixture** — no membership
   movement at all: ids and scope identical, but between the GET and the
   POST Compare accepts one member of a card rendered on the unfiltered
   `all` tab. The POST's `observed` still names that row `pending`, so
   the click is refused **409 `card_changed`** with
   `changed_prediction_ids` naming it and its `from`/`to`, and nothing is
   written — the accepted member stays accepted, no keyword is retracted,
   and no non-undoable `prediction_reject` is recorded. Three negative
   variants: a membership-only build (the earlier rule) passes the check
   and silently reverses the unseen acceptance, which is the regression
   guard; a build that skips the moved member and applies the rest fails,
   because it leaves the card partially resolved and computes Phase B's
   single keyword effect over a membership the user never saw; and a
   build that compares only the card's folded badge fails on
   `{accepted, pending, rejected}` versus
   `{accepted, rejected, rejected}`, which badge identically while
   retracting different keywords. A positive variant pins
   compare-and-swap rather than an is-it-decided check: "Reject all" on a
   card whose `accepted` member was *rendered* accepted proceeds and
   reconciles it. A
   **card-detail frozen-membership fixture** — the same transition
   against a card-detail POST whose body carries `id` plus
   `member_prediction_ids` returns the clicked card's members only, and
   the call that omits `member_prediction_ids` returns the grown
   component with `"expanded"` set; a
   **cache-transition-anchor-deleted fixture** — the same setup
   but with the anchor's rows deleted between the GET and the POST
   (e.g. a re-run rewrote the bucket) returns 400, the documented
   stale-handle failure mode (§2, "Anchor lookup and cache-transition
   safety"); a **historical-window fixture** — a legacy burst
   captured under `grouping_window_seconds = 600` with a 400s gap
   between consecutive frames resolves as one node regardless of the
   workspace's current effective `grouping_window_seconds`, because
   node identity reads no timestamp and no config value at all; a
   **timestamp-mutation invariance fixture** — two pre-Phase-0
   colliding legacy bursts of different species render as two nodes
   and two cards; `_refresh_photo_metadata` then rewrites
   `photos.timestamp` on rows of both bursts between the GET and the
   POST, and each `node_id` still resolves to exactly its own burst.
   This is the guard for the failure the capture-time partition could
   not close: a timestamp correction that *merged* two subsets left
   the lower-anchored handle resolving successfully onto the other
   subset's hidden rows (§2, "Capture time — also rejected");
   **filtered-view mutation-ID
   fixture** — with any of the four client-applied predicates active,
   the client's mutation POST carries
   `node_id` (not `card_id`), and a fixture POST that names a
   non-existent `node_id` or that carries both `card_id` and
   `node_id` returns 400; its **server-applied counterpart** — with
   only a `visual` clause (or only `collection_id`/`rules`) active, the
   POST carries `card_id` plus `member_prediction_ids` plus the
   server-applied scope, and the four client-applied scope entries
   `null`.
4. **Keyword canonicalization** (taxon-matched keyword reuse) + the
   "tags as …" transparency note + the `'accept'` value on
   **PR #1488's** `photo_keywords.source` column
   (`'manual'` > `'accept'` > `NULL`, §3 "Retraction requires
   provenance"). Depends on #1488 being merged; this phase adds the
   third value, converts `tag_photo`'s upsert from a coalesce to a
   precedence-max, and folds provenance in `_merge_keyword_into` and
   every other path that rewrites `photo_keywords.keyword_id` (a grep
   for writes to that table is part of this phase's checklist). The
   value lands here because
   this phase already owns the accept path's keyword write, and Phase 5
   is the first thing that can retract one — every accept made from
   Phase 4 onward therefore carries provenance by the time a reconciling
   reject could consume it. No backfill: pre-existing rows stay `NULL`
   and take the "disclose, don't strip" path forever. This phase lands
   **before** the
   cross-model accept broadening (Phase 5) so that the accept path's
   sibling loop cannot fragment keywords across name variants — see §3
   "Ordering constraint" for the full rationale. DB tests: a
   **provenance-write fixture** — an accept that tags a photo writes
   `source = 'accept'`, interactive tagging writes `'manual'` (a sidecar
   import leaves `NULL`, per #1488), and re-accepting an already-tagged
   photo does not downgrade an existing `'manual'` row to `'accept'`;
   a **merge-fold fixture** — a photo carrying an `'accept'`-owned
   destination keyword *and* a `'manual'`-owned duplicate keyword keeps
   `'manual'` on the surviving row after `_merge_keyword_into`, and a
   subsequent reconciling reject leaves that row in place and takes the
   disclosure path instead of untagging it; the mirror case (`NULL`
   destination, `'accept'` source) folds up to `'accept'` so the
   accept's own tag stays retractable after a synonym merge;
   **inat-id-translation fixture** — an existing "Eurasian Blue Tit"
   keyword linked to the local *Cyanistes caeruleus* taxa row is
   reused when a newly accepted *row* resolves to the iNat id for
   *Cyanistes caeruleus* (precedence 1 hits after `taxa.inat_id`
   translation); a fabricated collision case where `taxa.id` for
   taxon A equals `taxa.inat_id` for taxon B does *not* reuse
   taxon B's keyword when accepting a row for taxon A; an unknown
   iNat id (not in the local `taxa` table) falls through to
   precedence 2 rather than raising or reusing an arbitrary keyword;
   a `name:`-keyed row skips precedence 1 entirely and writes its raw
   label; a
   **variant-agreement fixture** — accepting a photo through the
   existing per-row `accept_prediction` primitive when a
   taxon-matched keyword ("Eurasian Blue Tit") already exists writes
   *only* the canonical keyword even if the accepted row's `species`
   string is "Blue Tit", so the Phase 5 sibling loop cannot introduce
   synonym fragmentation; a **first-accept convergence fixture** —
   with **no** keyword yet existing for *Cyanistes caeruleus*,
   accepting a "Blue Tit" row and a "Eurasian Blue Tit" row on the
   same photo yields exactly **one** keyword (the taxon's preferred
   common name), proving precedence 2 converges and that Phase 4 does
   not merely defer fragmentation to the first accept; a
   **no-card-caller fixture** — the same convergence holds when the
   accept is driven by Compare's `accept_subject_species`, which has
   no card, confirming the canonicalization is keyed on the row's
   taxon rather than on a card (§4, "Keyword written on accept"). The
   *mutation-scoped* half of §4 ("Resolved once per mutation, not once
   per photo") ships **in this phase, not Phase 5**: a multi-photo
   mutation already exists — `accept_prediction` on a grouped row tags
   every in-scope member photo (`db.py:18276-18290`) — so leaving the
   precedence per-photo would make Phase 4 fragment synonyms across a
   burst that today gets one string. Only Compare's
   `accept_subject_species` is genuinely single-photo (`db.py:18360-18364`).
   Phase 4 therefore ships cases 1–3 evaluated once over the call's
   resolved target set, plus a **grouped-accept convergence fixture**:
   a burst where member photo 1 already carries "Eurasian Blue Tit",
   member photo 2 is untagged, and a lower-`id` global "Blue Tit" exists
   — accepting the grouped row writes "Eurasian Blue Tit" to photo 2 and
   nothing to photo 1, and a build that resolves per photo writes both
   spellings and fails. **Depends on PR #1489** in addition to #1488:
   on `main` the keyword is created (`db.py:17947`) before the group is
   expanded (`db.py:18276`), so there is no target set to take a union
   over; #1489 already reorders these (`db.py:18159` / `18205` on
   `predictions-panel-in-browse`). Phase 4 lands after #1489 or repeats
   that reordering itself — see §4, "This adds a dependency on PR #1489".
   What is left for Phase 5 is only the *wider* union — photos drawn
   from more than one group — and the two-part disclosure that names
   both the written and the retained spelling.
5. **Merged-card rendering + cross-model accept/reject** + undo
   coverage. This phase turns the client over to `card_id`: the
   `getVisibleItems` dedup, merged-card rendering, the aggregate/`mixed`
   status rule, the filter-semantics fallback, and the modal's use of
   the card endpoint — §2's corollaries 2 and 4 — land here *together
   with* the mutation that can resolve a merged card, for the reason
   given in Phase 3. It also converts **every** entry point the
   enumeration in §3 ("Every mutation entry point, enumerated") marks as
   routed — per-card buttons, the toolbar `acceptAllPending`, the
   toolbar's `renderButtons` count, and the `A`/`S` keyboard handler —
   in the same phase, because a routed path left on the legacy endpoint
   for one release would half-accept merged cards for that release. The
   409 `card_changed` handling lands with them — the card-level inline
   notice naming the departed **or joined** count, the disabled modal
   actions on a `departed`/`expanded` detail read, and the toolbar
   rollup's worse-outcome summary (§2, "Shrinkage is a stale click, not
   a smaller click"). It
   is not a follow-up: shipping the server precondition without the
   notice converts a silent partial accept into a silently *ignored*
   click, which is the same failure wearing the other hat. The
   excluded rows of that table are explicitly *not* touched here.
   Depends on Phase 4
   being live so that the per-row keyword write resolves to the
   canonical keyword — otherwise the sibling loop across "Blue Tit" /
   "Eurasian Blue Tit" rows would tag both synonyms on the same photo,
   the exact fragmentation §3's "Ordering constraint" describes. It also
   widens the mutation-scoped half of §4 — the same cases 1–3 Phase 4
   already resolves once per call, now over a union whose photos come
   from more than one group — and adds the two-part
   `tags as "…" — N photos keep "…"` disclosure. The
   `sync.py` half of the retraction rule lands here rather than as a
   follow-up, because this is the phase that first queues a keyword
   delta in a workspace other than the acting one: the apply-time
   re-validation of `keyword_remove` (and its mirror on `keyword_add`)
   against the live `photo_keywords` set, alongside the restore-side
   cancellation loop (§3, "The pending queue is workspace-scoped and
   the association is not"). DB
   tests: accepting the merged card (unfiltered — POST carries
   `card_id`) flips both models' rows; undo restores both; reject
   mirrors; Compare's `accept_subject_species` matches across name
   variants and writes exactly one keyword per photo (asserting the
   Phase 4 dependency actually holds end-to-end); a
   **one-spelling-per-card fixture** — a card spanning photo 1 (already
   tagged "Eurasian Blue Tit") and photo 2 (untagged), where a global
   "Blue Tit" keyword with a lower `keywords.id` also exists: the accept
   writes **"Eurasian Blue Tit"** to photo 2 and nothing to photo 1, so
   exactly one spelling is written across the card. A build that resolves
   per photo picks the global lowest-id for photo 2 and fails — the
   regression guard for §4's "Resolved once per mutation". A second
   variant covers case 2: photo 1 carries "Eurasian Blue Tit", photo 2
   carries "Blue Tit", photo 3 is untagged, and a third global synonym
   with a still-lower id exists — photos 1 and 2 are untouched, photo 3
   gets the lowest-id spelling **present on the card**, the third
   synonym is never written, and the disclosure names both the written
   string and the retained one; a build that names only the written
   string fails the disclosure assertion;
   **transitive-component accept fixture** — accepting the A-B-C card
   (POST carries `card_id`, no filter scope) flips every pending row on
   photos 1-4 for the matching taxon and leaves other taxa untouched;
   undo restores every flipped row (including C's); **scoped-mutation
   fixture** — with a collection filter that excludes group D (same
   taxon, overlapping photos), the accept POST carries `card_id` plus
   the collection scope and D's rows stay pending; without the filter,
   D merges and is accepted; a **min-confidence-scoped fixture** —
   `minConfidence` is set above group E's confidence so E is hidden and
   would otherwise bridge two visible groups A and F through a shared
   photo, the accept POST from a filtered view carries `node_id`
   (naming A's node) plus `min_confidence` and only A is resolved,
   E and F stay pending; accepting F likewise resolves only F; a
   **status-scoped fixture** — with `currentTab` = `pending`, an
   already-accepted sibling row G on a bridging photo is excluded from
   the displayed component and a per-node `node_id` accept plus
   `status = "pending"` in the scope tuple leaves G untouched and does
   not stitch unrelated groups together; a **visual-scoped fixture** —
   an active `VireoFilter.getVisual()` clause whose match set excludes
   photo `p*`, on which a same-taxon group V would otherwise
   bridge visible groups A and F. Because the clause is server-applied,
   this is a merged-card view: the accept POST carries **`card_id`**
   (A's own component, V having never entered the graph) plus
   `member_prediction_ids` plus the same `visual` JSON payload the GET
   sent, the server re-runs
   `_apply_visual_to_rules` on the mutation path and resolves against
   the identical matched-photo-id set, and only A is resolved — V
   (on the excluded photo) and F stay pending; accepting F likewise
   resolves only F. The negative half is the load-bearing one: an
   otherwise identical POST that **omits** `visual` rebuilds over the
   unscoped row set, re-admits V, and accepts A+V+F in one click — the
   regression guard for "server-applied filters travel on every
   re-expanding call"; a **visible-sibling-node fixture** — under an
   active filter that forces per-node fallback (e.g.
   `currentModel = 'BioCLIP-2.5'` chip active but the taxon-key merger
   would otherwise unite it with an iNat21 node, or a similarity re-run
   that split one component into two visible nodes A and B), A and B
   have the same `taxon_key` and share photo `p*`, both are visible, no
   row is hidden; the accept POST for A's `node_id` flips only A's own
   rows (including its row on `p*`), B's row on `p*` and all of B's
   other rows stay pending, and a subsequent accept POST for B's
   `node_id` flips only B's rows; the pair reaches full resolution in
   exactly two clicks (matching the two cards the user sees) and never
   in one; running the same scenario without a filter routes the POST
   through `card_id` and one click accepts both nodes (regression guard
   that the bifurcation is not a permanent loss of the transitive-merge
   behaviour, only a scope-honest suppression while a filter is active);
   a **filtered-mutation shape fixture** — a POST that carries both `card_id` and `node_id` is
   rejected 400; a POST that carries a `node_id` unknown to the server
   (e.g. after a re-run rewrote group IDs) is rejected 400; a stale
   POST that omits the scope tuple is not reinterpreted as a card at
   all; a **legacy-payload fixture** — a bare `prediction_id`/`group_id`
   POST (a pre-deploy Review page, or a deep-link button) resolves the
   clicked prediction plus its own group's siblings **restricted to that
   prediction's `classifier_model`**, exactly as `accept_prediction`
   does today, and leaves the other model's overlapping same-taxon rows
   pending — the guard against a stale click silently gaining
   merged-card reach through version skew; a POST that carries a scope narrower
   than the server's full component cannot exceed the displayed
   membership; a **hidden-sibling-node fixture** — a legacy
   colliding bucket holding two species whose *first* node's rows are
   entirely hidden by `min_confidence` (and, in a second variant, by
   the status tab): the `node_id` the client minted for the surviving
   node still resolves on the POST and mutates exactly that node —
   guarding against the 400-on-a-valid-click that a positional
   `subset_index` plus a filter-scoped partition rebuild would have
   produced, and therefore the regression guard for keying node identity
   on intrinsic row columns rather than on anything the query scope can
   move (§2, "Node identity is a pure function of immutable row
   columns"); a **mixed-status card fixture** — an accepted BioCLIP-2.5 row and a
   pending iNat21 row share one card (same taxon, overlapping photos);
   under every representative-row sort order tried (species-string asc,
   confidence desc, prediction-id asc) the aggregate rule renders the
   card as *mixed* ("Mixed — 1 pending · 1 accepted") with Accept all /
   Reject all visible; accepting resolves
   both members via the `card_id` sibling pass; the card then renders
   as *accepted* with only the undo hook (no accept/reject action),
   guarding §2 "Client changes" against the representative-row
   regression that would silently collapse the pending duplicate; a
   **mixed-terminal card fixture** — one accepted and one **rejected**
   same-taxon member, no pending member, built directly in the DB so no
   card click created it: the card renders `mixed` with both actions,
   is absent from every "resolved" count, and "Accept all" leaves all
   members accepted (symmetric variant for "Reject all"); a
   **reconciling-accept fixture** — a card with one pending and one
   rejected member, accepted via `card_id`: **both** end accepted, so
   the mutation cannot itself produce a mixed card, which the
   `pending`-only sibling scan of the earlier revision failed; a
   **prior-status undo fixture** — undoing the accept of a
   `{pending, accepted, rejected}` card restores each member to exactly
   the status it held before the click (pending → `pending`, the
   reconciled member → `rejected`, the pre-existing accepted member →
   `accepted`, demoted alternatives → `alternative`), which the current
   blanket reset (`db.py:18925-18952`) fails on the second and third
   assertions; a variant replays a **legacy** `prediction_accept` entry
   with no prior-status payload and asserts it still undoes through the
   old branch; the history description names the overridden count; a
   **sibling-snapshot fixture** — a detection carrying one pending top-1
   row and two `alternative` rows on the same
   `(detection, classifier_model, labels_fingerprint)`: accepting an
   alternative demotes the other two through `accept_prediction`'s
   sibling loop (`db.py:17137-17162`), and undo restores each of them to
   the status it held before the click, **not** to `pending` and not
   left at `rejected` — the regression guard for building the snapshot
   from `affected` alone, which never sees those rows (§3, "The capture
   point is not `affected`"); a
   **reconciling-reject
   keyword fixture** — "Reject all" on a card whose accepted member
   tagged the photo with `source = 'accept'` flips the status *and*
   untags that keyword via an undoable `keyword_remove`; three negative
   variants keep the keyword and surface the disclosure instead of
   silently disagreeing with the badge — `source = 'manual'`,
   `source IS NULL` (the pruned/legacy case: assert the keyword survives
   even after `_prune_edit_history` has deleted the original
   `prediction_accept` entry, which is the regression guard for not
   sourcing provenance from the bounded edit log), and another live
   non-rejected prediction still asserting the taxon; a
   **`name:`-keyed retraction fixture** — a card whose §1 key is
   `('name', 'blue tit')` (an unresolved custom label, no `taxa` row
   for it): an earlier accept stamped `source = 'accept'` on the
   raw-label `photo_keywords` row per §4 precedence 2. A subsequent
   "Reject all" retracts that keyword via an undoable `keyword_remove`
   and cancels its pending `keyword_add` across every workspace,
   exactly as the `('taxon', T)` case does. A build whose retraction
   enumeration keys only on `keywords.taxon_id` finds no row, leaves
   the keyword on the photo, and fails — this is the regression guard
   for §3 Phase B, "`name:`-keyed cards enumerate by folded keyword
   name". Negative variant: the same card whose row is
   `source = 'manual'` (the user typed the same label) takes the
   disclosure path with the workspace-aware wording, and a `taxon_id`
   build silently strips it. **Cross-key liveness variant, the
   load-bearing one:** photo P carries a single `photo_keywords` row
   that *both* a `('name', 'blue tit')` prediction and a
   `('taxon', 13094)` card claim — the taxon card's §4 precedence-1
   lookup resolves to that same keyword row. Rejecting the taxon card
   while the `name:` row is still pending keeps the association and
   discloses why; rejecting the `name:` card while a `taxon:` row is
   pending likewise keeps it; only once both are rejected is it
   retracted. A build that tests liveness by card-key equality — the
   natural reading of "same §1 key as the card" — strips the tag on
   the first of the two rejects and fails, which is the regression
   guard for §3, "Liveness is asked per claimed association, not per
   card key"; a
   **case-2 multi-synonym retraction fixture** — a card whose earlier
   accepts (under §4 case 2) left `source = 'accept'` "Blue Tit" on
   photo P1 and `source = 'accept'` "Eurasian Blue Tit" on photo P2:
   "Reject all" now retracts *both* synonyms (each recorded as its own
   undoable `keyword_remove`) and the disclosure names both strings,
   `untagged "Blue Tit" and "Eurasian Blue Tit" from 2 photos`. A build
   that resolves the reject to the single card keyword and untags only
   that spelling leaves the other synonym stuck on its photo and fails
   — the regression guard for "the single-string rule bounds writes
   and disclosure, not removals" (§3, Phase B). Two negative variants:
   one where P2's "Eurasian Blue Tit" row is `source = 'manual'` (the
   user typed it), which is left in place and takes the disclosure
   path ("kept 'Eurasian Blue Tit' on 1 photo — you added this keyword
   yourself") while P1's `'accept'` row is still stripped; and one
   where a third same-taxon synonym exists globally on some *other*
   photo but has no `photo_keywords` row on P1 or P2, which is
   untouched — enumeration is per affected photo, not per taxon-scoped
   keyword; a
   **scope-divergent liveness fixture** — the case a photo-local key
   cannot see. Photo P is shared by workspaces A and B; every prediction
   row on P labelled `L` carries `scientific_name = NULL`, while B's row
   set also holds a `pending` row labelled `L` on a *different* photo
   whose `scientific_name` resolves to taxon `T`, so B renders that card
   `taxon:T`-keyed. In A, "Reject all" on the card owning P's
   `source = 'accept'` "Eurasian Blue Tit" row — a synonym only a taxon
   key claims — must **keep** the keyword and disclose that B still
   asserts it. A build that resolves the liveness key from P's rows alone
   gets `('name', L)`, finds no claim, strips the keyword and fails,
   leaving B's Review rendering a pending card over a photo that lost its
   tag: the regression guard for §3, "the key those pairs are matched on
   cannot be §1's". Mirror variant asserting the superset direction
   rather than assuming it: a catalog-wide conflict on `L` (two rows, two
   taxa) puts both taxa in `Taxa(L)`, so both taxa's synonym rows on P
   count as claimed and are kept. Third variant keeps the rule from
   degenerating into "never retract": once B's row is `rejected` too and
   nothing claims the association under either clause, the keyword *is*
   retracted; a
   **cross-workspace liveness fixture** — a photo P appears in
   workspaces A and B (both have `workspace_folders` rows for P's
   folder), each carrying its own `prediction_review` row for a
   same-taxon prediction, both `pending`. In workspace A the user runs
   "Reject all" on the card containing A's row: A's `prediction_review`
   status flips to `rejected`, but the keyword (accept-owned from an
   earlier click) **stays** on P because B's row is still a live
   assertion catalog-wide, and A's disclosure names the workspace it
   survives in — "kept 'Blue Tit' — still predicted in 1 other
   workspace (B)" — rather than a bare count or zero. A build whose
   liveness query filters by `workspace_id = active` strips the
   keyword and fails. Symmetric variant: once B also rejects, the
   keyword is retracted on the *second* reject, from B's workspace,
   because the catalog-wide live set is now empty. **Implicit-pending
   variant, and the load-bearing one:** the same build with **no
   `prediction_review` row for B at all** — the ordinary state, since
   `add_prediction` writes no row for a pending prediction
   (`db.py:15909-15918`) and B has simply never been clicked in. B's
   Review still renders the prediction `pending` through
   `COALESCE(pr_rev.status, 'pending')`, so A's reject must keep the
   keyword exactly as in the explicit-row case. A build whose liveness
   query starts `FROM prediction_review` — rather than crossing
   `predictions` with the photo's workspaces and left-joining — sees an
   empty live set, strips the keyword, and fails. Third variant pinning
   the other direction so the rule cannot be satisfied by "never
   retract": B holds an explicit `rejected` row and every other
   workspace containing P holds one too, so the live set really is
   empty and the keyword *is* retracted; a
   **cross-workspace pending-add fixture** — an accept in workspace A
   tags P (`source = 'accept'`) and queues A's `keyword_add "Blue Tit"`,
   unsynced. The final live assertion is then rejected from workspace B.
   Assert three things: the global `photo_keywords` row is gone, **A's
   pending `keyword_add` is gone**, and a `keyword_remove` is queued in
   both A and B. A build that calls `remove_pending_changes` on its
   default (active) workspace leaves A's add in place; driving A's sync
   afterwards then writes "Blue Tit" back into P's sidecar, and the
   fixture fails on the sidecar contents, not just the queue. Negative
   variant tying the cancellation to the lattice rather than to a
   parallel rule: between the accept and the reject, the user hand-tags
   "Blue Tit" on P from workspace B, folding the row's `source` up to
   `'manual'`. The reject now retracts nothing — so it must cancel
   nothing, and A's queued add survives to write the keyword the user
   asked for. A build that cancels pending adds for the rejected taxon
   independently of the retraction decision drops the user's own keyword
   from the sidecar and fails; a
   **cross-workspace stale-remove fixture** — a photo P shared by
   workspaces A and B, an accept then reject of "Blue Tit" from A
   queues a `keyword_remove` in both A and B (the queue-side rule
   above). A syncs, stripping the term from P's sidecar; the user then
   restores the keyword in A — first via `_apply_undo` on the reject,
   then, in a second variant, via `tag_photo` (`app.py:8221`), and in
   a third, via a subsequent accept whose Phase-B keyword resolution
   re-writes the term. In all three, assert **B's queued
   `keyword_remove` is gone** at the moment of the restore, so that
   driving B's sync afterwards is a no-op on the sidecar. A build that
   cancels only A's pending remove leaves B's copy alive; B's next
   sync strips the restored user-authored tag back off the file, and
   the fixture fails on **sidecar contents** rather than on B's queue
   state. Negative variant pinning the loop to `remove_pending_changes`
   rather than to a proposed global-scope row: assert that
   `count_pending_changes` in a *third* workspace C that also contains
   P shows zero pending `keyword_remove` for the term throughout, so
   the fix is not "make the queue row workspace-null" (which would
   surface in C's summary); an
   **apply-time re-validation fixture**, covering the copies the
   restore loop cannot reach — B's `keyword_remove` is queued, then
   P's folder is removed from B (so no
   `(photo_id, value)`-keyed restore loop enumerating *current*
   workspaces would find B), and the user restores the keyword in A;
   driving B's sync leaves "Blue Tit" in the sidecar and deletes B's
   pending row rather than applying it. A build that trusts the queue
   strips a restored, user-authored tag and fails on sidecar contents.
   Two negative variants keep the check from degenerating into "never
   remove": a genuine removal, where the catalog association really is
   gone, still applies in every workspace holding a copy; and a
   **paired remove+add normalization rename** (remove and add sharing
   one `keyword_match_key` in the same batch) still performs its
   flat-only removal instead of being discarded as stale, as does a
   `keyword_remove_flat` queued by `repair_duplicate_photo_species`
   while the hierarchical association survives; a
   **two-phase-ordering fixture** — "Reject all" on an
   `{accepted, pending}` card whose two members assert the **same taxon
   on the same photo**, the accepted one having tagged it
   `source = 'accept'`: both rows end `rejected` *and* the keyword is
   removed via an undoable `keyword_remove`. A row-at-a-time
   implementation that evaluates the retraction guard right after the
   first flip sees the still-pending sibling as a live assertion, keeps
   the keyword, and fails — this is the regression guard for §3, "A card
   mutation writes every member status before it decides any keyword
   effect". Its mirror is the load-bearing half and rules out the
   over-broad repair: a third same-taxon row on that photo which the
   mutation does **not** touch (excluded by the scope tuple, so outside
   `server_members`) stays pending, and the keyword must be
   **kept**, with the "still predicted on 1 photo" disclosure — so the
   rule is "evaluate after the writes", not "ignore every sibling". A
   third variant asserts the client-side half of the closure clause:
   "Reject all" issues **one** card mutation, and a build that
   decomposes the card into one request per member reproduces the leak
   and fails; a
   **status-totality fixture** — all seven non-empty
   member-status sets map to a defined badge and action set; a
   **toolbar Accept-All fixture** — a view holding one `{pending}` card
   and one `{pending, rejected}` merged card: clicking the toolbar
   "Accept All" leaves **every member of both cards accepted** and zero
   `mixed` cards on reload, which the row-iterating
   `acceptAllPending` (`review.html:1618-1633`) fails by leaving the
   merged card's rejected member rejected; the same fixture asserts the
   button reads "Accept All (2)" — cards, not the 3 raw pending rows —
   and that a view whose only card is `{accepted, rejected}` still
   *shows* the button rather than hiding it on a zero pending-row count;
   under an active filter the same click issues one `node_id` mutation
   per displayed node with the scope tuple, and rows the filter hid stay
   pending; an **actionable-set fixture** — the `all` tab holding four
   cards, one each `{pending}`, `{accepted}`, `{rejected}` and
   `{pending, rejected}`: "Accept All" issues **exactly two** mutations
   (the pending and the mixed card), the button reads "Accept All (2)",
   the terminal accepted and rejected cards are byte-identical
   afterwards — in particular the rejected card is still rejected, which
   a loop over `getVisibleItems()` reverses — and the summary names the
   overridden rejection inside the mixed card; a **keyboard-shortcut
   fixture** — `A` on the first visible merged card accepts every member
   of that card and only that card, matching what a click on its "Accept
   all" button does, and its two counterparts on the same actionable
   set: with a `mixed` card sorted first, `A` targets *it* (a
   `status === 'pending'` filter would skip to a later card or no-op
   entirely), and on a view whose visible cards are all terminal both
   `A` and `S` are no-ops rather than mutating `getVisibleItems()[0]`;
   an
   **excluded-path fixture** — `acceptAlternative` on a member of a
   merged card still POSTs the legacy per-prediction endpoint, touches
   only that detection's rows, and leaves the card rendering `mixed`
   with both reconciling actions available (the admitted consequence of
   the exclusion in §3's entry-point table, asserted rather than
   discovered).

## Test plan

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py \
  vireo/tests/test_app.py vireo/tests/test_photos_api.py \
  vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py \
  vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```

plus the new tests per phase above. Manual verification: reproduce the
original state (both classifiers over the same burst with the two label
variants), confirm one card, accept it, confirm zero pending rows remain for
either model, undo, confirm both return.
