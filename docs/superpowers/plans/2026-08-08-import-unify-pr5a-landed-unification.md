# Import Unification PR 5a: Landed Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify the two `landed` ledgers behind a `_LandedFile` dataclass, fold remote `adopted_paths` into `landed` (deleting the separate validation pass), and unify rollback via origin-switching `_reclassify_landed_failed` twins — deliberately shipping three documented behavior flips that align remote to local.

**Architecture:** First half of the spec's "PR 5 — state consolidation" (spec: `docs/superpowers/specs/2026-08-06-import-path-unification-design.md`). A 2026-08-08 inventory (embedded below) concluded the full PR 5 should split: (a) `_LandedFile` + (b) adoption fold + (c) shared rollback are ONE semantic unit — you cannot fold adoptions without the `origin` field, and cannot fold them without origin-switching rollback, or `copied` goes negative on adopted-entry failures — while (d) the `_record_checker` hoist + (e) `_ImportRunState` are a mechanically different ~350-one-token-line diff that would bury the behavior flips (that's PR 5b, separately planned). Task 1 amends the spec to record the split.

**Tech Stack:** pytest; the behavior-parity harness; `dataclasses`.

---

## The three deliberate behavior flips (say ALL of them in the PR body)

1. **Divergence 10 (spec-intended):** remote adopted rows gain the `hash_status='ok'` stamp under `verify_by_hash` (they flow through the landed stamping loop now). The characterization pair `test_{local,remote}_adoption_uncataloged_dest_twin_current_behavior` flips by design.
2. **Working-copy source override:** remote adopted files gain a card-side `wc_source_paths` entry (local adoptions always had one because they live in `landed`); WC extraction reads the card instead of the mount for those files.
3. **Failure reporting for adopted-file validation failures:** `unsafe_files[].path` becomes the mount dest path (was the card source path) and reason wordings become local's — user-visible in `result["errors"]`.

All three align remote to local; each gets pinned by a test in this PR.

## Context for a zero-context engineer

- Repo root: `/Users/julius/conductor/workspaces/vireo/nagoya`; branch `import-unify-pr5a-landed-unification`... actually run `git branch --show-current` — the branch is `import-unify-pr5-state-consolidation` (created before the split decision; keep it, the PR title clarifies). Tracks origin/main at the PR-1434 merge `ab3ee86c`. Run tests from repo root; commit per task.
- `vireo/import_job.py` (4889 lines): remote `_run_remote_import_job` 1235-3221; local `run_import_job` 3224-4889. Line numbers below verified at `ab3ee86c`.
- **Tuple shapes today:** remote `landed` 5-tuple `(dest_path, card_source_str, src_hash, src_size, src_mtime_ns)` (decl 2263, sole append 2467). Local 6-tuple `(dest_file, hash, source, origin, size, mtime_ns)` (appends: adoption 4177-4180 with `"skipped_duplicate"`, fresh copy 4213-4216 with `"copied"`); the decl comment at 3798-3801 is STALE (claims a 3-tuple) — fix it when touching.
- **Remote landed sites** (all must convert to attribute access): decl 2263; append 2467; guard 2488; `landed_paths` 2489; scan-failure rollback 2536-2543; stamping loop 2583; four in-place filters `landed = [e for e in landed if e[0] != dest_path]` at 2666, 2685, 2766, 2787; diff-loop read 2964 (`entry[2]` = hash); WC fill 3034.
- **Local landed sites:** decl 3802; appends 4177/4213; mount-lost rollback 4282-4290; guard 4316; `landed_paths` 4317; pre-scan capture 4332-4345; scan-failure rollback 4369-4373; stamping loop 4437-4439 (`entry[1]` = hash); diff loop 4584-4606; WC fill 4656-4671 (`entry[2]/[4]/[5]`).
- **Local `_reclassify_landed_failed`** (def 3571-3593): switches on `entry[3]` origin; `"copied"` → `copied -= 1; verified -= 1; _counts["copied"] -= 1` (verified UNCONDITIONAL — copy_and_hash_verify always verifies); `"skipped_duplicate"` → `skipped_duplicate -= 1; _counts[...] -= 1`; then `_fail(rel, dest_path, reason)`. Six call sites: 4284, 4370, 4503, 4510, 4552, 4560 (all but the first two also add to `reclassified_landed_paths`, decl 4394).
- **Remote open-coded rollbacks** to replace: R1 scan-failure 2536-2543; R2 2651-2669 (rehash disagrees); R3 2671-2688 (scan≠src hash); R4 2750-2769 (companion mismatch); R5 2781-2790 (not cataloged) — each does `copied -= 1; if params.verify_by_hash: verified -= 1; _counts["copied"] -= 1` + a landed filter. R6-R8 (adopted-pass rollbacks 2859-2869, 2910-2921, 2923-2932: `skipped_duplicate/dup_skipped/_counts` decrements + `failed_adopted_paths.add`) are DELETED by the fold.
- **`verified` guard difference:** local books `verified` for every copy; remote only under `params.verify_by_hash` (booked at 2464-2466). The unified helper bodies stay byte-identical by having each function define ONE flag near the top of the batch machinery: local `verified_counted_for_copies = True`, remote `verified_counted_for_copies = params.verify_by_hash`, and the helper does `if verified_counted_for_copies: verified -= 1`. (PR 5b/7 collapses this into the transport's `attests_bytes`.)
- **`adopted_paths` (remote-only, dies in this PR):** decl 1830 (rationale comment 1810-1829); populated ONCE at 2160 inside the collision-adopt branch (which ALSO does `dup_skips.append((source_file, False))` at 2150 — see the hazard below); reset at 2254 (mount-lost); guard/`scan_files` 2488/2494; pre-scan capture iterates `scan_files` 2496-2505; validation pass 2793-2937 (no-row rollback 2854-2869; conditional re-hash `if is_companion or scan_h is None:` 2879-2921; scan-hash mismatch 2922-2932; terminal `is_companion → raw_companion_invalidations.add(row_id)` + `imported_photo_ids.add(row_id)` 2933-2937); `failed_adopted_paths` decl 2582, read in diff loop 2963-2969.
- **⚠️ THE DOUBLE-ROLLBACK HAZARD (highest risk):** remote adoption books into `dup_skips` (line 2150 — NOT 2149, which is the `_counts` increment; match the quoted code, not the number) AND — post-fold — into `landed`. The mount-lost block rolls back `dup_skips` (2232-2244, `skipped_duplicate -= 1`) and the fold adds a `landed` rollback for the same file (`skipped_duplicate -= 1` again) → negative counters. **The fold MUST delete the `dup_skips.append` line (2150)** (local's adoption at 4174-4183 has no `dup_skips.append`). No existing test covers mount-detach-during-remote-adoption — Task 3 writes that pin FIRST.
- **`dup_skipped` is dead state:** remote-only, written at 1807/1911/2007/2087/2116/2148 and decremented in rollbacks, NEVER read. Task 2 deletes it outright.
- **Decision-7 stat covers adoptions:** `src_size/src_mtime_ns` are computed at 2044-2053, before the collision walk — live at the adoption site. The folded adoption entry uses them directly; no new stat.
- **Stamping-loop semantics for folded adoptions:** the landed loop re-hashes when `scan_h is None and src_h_norm is not None` (2635) and stamps `hash_status='ok'` under verify (2691-2697); the old adopted pass re-hashed when `is_companion or scan_h is None` (2879). For companions `is_companion` forces the re-hash in both worlds (the landed companion branch re-hashes unconditionally at 2732-2740) — equivalent. One dropped read: a ZERO-BYTE adopted file (`src_h_norm is None`, `scan_h is None`) got a mount re-read in the old pass and won't in the landed loop; both normalize to None so behavior matches — mention in the PR body, don't let a reviewer discover it.
- **Failure-subject flip mechanics:** R6-R8 called `_fail(rel, adopt_source, ...)` (card path, adoption wording); the landed loop's paths call `_fail(rel, dest_path, ...)` (mount path, local wording). This is flip 3.
- **Tests that flip or need edits** (`vireo/tests/test_import_job.py`, 12462 lines):
  - Adoption characterization pair: local 9650-9689 (pins `("2026/2026-07-03","DSC_0001.jpg",hash,"ok")` + cross-path equality minus db_photos), remote 9696-9716 (pins `hash_status=None`; docstring names the PR-5 flip). Post-fold: remote pins `"ok"`, the pair collapses, and `_ADOPTION_SCENARIO` (9584) moves back into `_BEHAVIOR_PARITY_SCENARIOS` (9556-9578, delete the DIVERGES comment at 9564-9565); update the two meta-tests (9588-9599, 9620-9648) in lockstep.
  - dest_read_cancel 2x2 (11950-12463): the remote-adoption test (12085) KEEPS its geometry (adoption now feeds the `landed` term exactly like the local adoption test at 12200 describes) — rewrite its docstring (12085-12092) + comment (12181-12186); the remote-fresh test's guard-text references (12352-12358, 12444-12455) update to `if landed and not dest_read_cancelled:`; runner docstring 11964.
  - `test_remote_import_invalidates_raw_caches_when_adopted_jpeg_pairs` (5555-5650): flow moves from the adopted pass to the landed companion branch; must still pass unchanged (its assertions are behavioral). Run it FIRST after the fold.
  - Docstring sweeps: ~9 test-file blocks reference `adopted_paths` (9658, 9702, 10910, 11006, 11964 + the 2x2 four); ~12 production comments (1580, 1810-1829, 1850-1854, 2218-2220, 2270-2273, 2496-2499, 2576-2581, 2793-2817, 2959-2962, 3546, 4254, 3798-3801).
- Preflight mirrors in app.py: the collision/adopt DECISION walk is untouched (only bookkeeping changes) — but state it carefully in the PR body since the adopt branch's bookkeeping lines change.

### Task 0: Branch sanity

- [ ] `git branch --show-current` (expect `import-unify-pr5-state-consolidation`); `git log --oneline -1` → `ab3ee86c` or newer. Baseline: full file `python -m pytest vireo/tests/test_import_job.py -q` → 218 passed, 1 skipped.

### Task 1: Spec amendment — record the 5a/5b split

- [ ] **Step 1:** In the spec's PR-sequence entry 5 ("PR 5 — state consolidation"), amend to record the split: 5a = `_LandedFile` + adoption fold + origin-switching rollback (this PR, with the three flips listed); 5b = `_record_checker` hoist + `_ImportRunState` (separate mechanical PR). Keep the original content, add the split note with the 2026-08-08 date and one-line rationale (the fold trio is semantically inseparable; the state-object rename diff would bury the flips).
- [ ] **Step 2:** Commit: `"Spec: split PR 5 into 5a (landed unification) and 5b (state object) with rationale"`

### Task 2: Delete dead `dup_skipped` state (mechanical, own commit)

- [ ] **Step 1:** Verify deadness yourself: `grep -n "dup_skipped" vireo/import_job.py` — every hit is an assignment (`= 0`, `+=`, `-=`); none is a read. If ANY read exists, STOP and report.
- [ ] **Step 2:** Delete all `dup_skipped` lines (decl 1807 and its comment if solely about it; increments 1911, 2007, 2087, 2116, 2148; decrements 2234, 2860, 2911, 2924).
- [ ] **Step 3:** Full file → 218 passed, 1 skipped. Commit: `"Remove dead remote dup_skipped counter"`
- Line numbers BELOW this point assume this deletion happened — re-grep, don't trust offsets.

### Task 3: Pin mount-detach-during-adoption rollback (both paths, pre-fold)

- [ ] **Step 1:** Write `test_remote_import_mount_detach_after_adoption_rolls_back_once` — geometry: a 2-file batch where file A adopts (byte-identical file pre-seeded at the template mount path, uncataloged) and file B is fresh; a `_unmounted_since_baseline` spy (mirror the existing mount-lost tests ~11000s — read one) flips detached AFTER the per-file loop (so the post-loop probe at ~2224 sees it). Assert: `skipped_duplicate == 0` (rolled back exactly once, not negative), `copied == 0`, `failed == 2`, `safe_to_format is False`, and `result["skipped_duplicate"] >= 0` trivially via the exact-0 pin. Also write the local mirror `test_local_import_mount_detach_after_adoption_rolls_back_once` (local adoption rollback runs via `_reclassify_landed_failed` on `landed`) — identical geometry, identical assertions.
- [ ] **Step 2:** Run both. Expected: GREEN today on both paths (remote rolls back adoption via `dup_skips`; local via `landed`). These pins are what catch the double-rollback if the fold forgets to delete the `dup_skips.append`. If either is RED today: STOP, report — a pre-existing counter bug is a finding.
- [ ] **Step 3:** Full file → 220 passed, 1 skipped. Commit: `"Pin single-rollback of adoptions on mount detach (both paths)"`

### Task 4: `_LandedFile` + rollback unification (no behavior change)

- [ ] **Step 1:** Add near the top of `vireo/import_job.py` (module level, after `ImportParams`):

```python
@dataclass
class _LandedFile:
    """One file this batch landed (fresh copy/transfer) or adopted.

    ``verified_hash`` is the hash the import attests is at ``dest_path``
    (copy-time hash locally; card-side hash remotely). ``origin`` is
    "copied" or "skipped_duplicate" (adoption) and drives rollback
    accounting in ``_reclassify_landed_failed``.
    """
    dest_path: str
    verified_hash: str | None
    source_path: str
    origin: str
    src_size: int | None
    src_mtime_ns: int | None
```

(`from dataclasses import dataclass` — check the import block.) Field order/names match the spec's Target-architecture sketch.

- [ ] **Step 2 (local first — smaller):** convert every local site to `_LandedFile` attribute access (appends 4177/4213 become keyword-arg constructions; `entry[0]`→`.dest_path`, `entry[1]`→`.verified_hash`, `entry[2]`→`.source_path`, `entry[3]`→`.origin`, `entry[4]`→`.src_size`, `entry[5]`→`.src_mtime_ns` at the sites listed in Context). Fix the stale decl comment 3798-3801. Run the full file → 220 passed (no behavior change).
- [ ] **Step 3 (remote):** convert remote sites (`entry[0]`→`.dest_path`, `entry[1]`→`.source_path`, `entry[2]`→`.verified_hash`, `entry[3]`→`.src_size`, `entry[4]`→`.src_mtime_ns` — NOTE the index mapping differs from local; the append at 2467 becomes `_LandedFile(dest_path=dest_path, verified_hash=src_hash, source_path=str(sf), origin="copied", src_size=sz, src_mtime_ns=mt)`). Run full file.
- [ ] **Step 4 (rollback unification, remote):** add `verified_counted_for_copies = params.verify_by_hash` near the remote ledger decls (and `= True` near local's, using it inside local's helper in place of the bare `verified -= 1` — bodies now byte-identical). Add the remote nested `_reclassify_landed_failed` (copy local's def verbatim, including docstring) plus a remote `reclassified_landed_paths = set()`; replace R1-R5's open-coded blocks with helper calls + `reclassified_landed_paths.add(...)`; DELETE the four in-place `landed = [...]` filters and instead skip `reclassified_landed_paths` members at the three downstream readers (diff-loop `changed_candidates`, WC fill, and — verify whether the stamping loop itself re-reads landed after filtering... it iterates `list(landed)` snapshot, so no). This adopts local's mechanism per the inventory's coupling-4 recommendation. Preserve each R-site's exact `_fail` reason strings.
- [ ] **Step 5:** Full file → 220 passed, 1 skipped (pure refactor; the parity net + summary strings are the check). Commit: `"Unify landed ledgers behind _LandedFile; origin-switching rollback on both paths"`

### Task 5: The fold (three flips land here) — TDD via the flipping pins

- [ ] **Step 1 (RED):** update the remote adoption characterization test (9696-9716): pin `hash_status` `None` → `"ok"`, rewrite the docstring to past tense (folded in PR 5a). Run it — MUST FAIL (still None). This is the fold's red phase.
- [ ] **Step 2 (implement):** in the remote adoption branch (~2146-2166):

  (2a) **HOIST THE `landed` DECLARATION FIRST.** Remote `landed` is declared at 2263 — AFTER the per-file loop containing the adoption branch and AFTER the mount-lost block. Without hoisting, the fold's append raises `UnboundLocalError` on a first-batch adoption, and on later batches appends to the previous batch's list that the 2263 re-init then wipes. Move the decl (with its shape comment, updated for `_LandedFile`) up to the per-batch state block (~1830-1855, next to `dup_skips`/`dup_dirs`), and delete the re-init at 2263. Local needs no such move (its decl at 3802 precedes its loop).

  (2b) Replace `adopted_paths[cand_mount] = (source_file, src_hash)` with `landed.append(_LandedFile(dest_path=cand_mount, verified_hash=src_hash, source_path=str(source_file), origin="skipped_duplicate", src_size=src_size, src_mtime_ns=src_mtime_ns))`; **DELETE the `dup_skips.append((source_file, False))` line in the adoption branch** (the double-rollback hazard — cite Task 3's pins in the commit message); keep `claimed_basenames`/`_record_checker` lines.

  (2c) **ADD the mount-lost `landed` rollback — it does NOT exist remotely.** The remote mount-lost block (2231-2261) rolls back `dup_skips`/`to_transfer` only; there is no `landed` rollback today (nothing could be in `landed` at that point pre-fold). Post-fold, adopted entries ARE in `landed` when the post-loop probe fires. Add the local mirror (see local 4282-4290): `if mount_lost and landed:` → `_reclassify_landed_failed(rel, entry, <local's shadow-landing reason>)` per entry → `landed = []`. Without this, adopted entries keep their skipped_duplicate count on a dead mount and the batch proceeds to scan it — Task 3's pins go red on `failed == 2` if you forget, but do not rely on the pin to design the fix.

  (2d) Then excise the machinery: `adopted_paths` decl + rationale comment (adapt the 1810-1829 comment onto the fold site), mount-lost reset of `adopted_paths` (2254), guard → `if landed and not dest_read_cancelled:`, `scan_files` → `landed_paths` everywhere (pre-scan capture, scan restrict_files), DELETE the whole validation pass (2793-2937) — its checks are subsumed: direct-row/companion handling by the landed stamping loop (adoptions flow through it now; the companion branch's `raw_companion_invalidations.add` covers the old `is_companion` add), rollbacks by the helper (which fires `skipped_duplicate` decrements for `origin="skipped_duplicate"`), `failed_adopted_paths` decl + diff-loop filter (replaced by `reclassified_landed_paths`), `changed_candidates`' adoption arm (landed covers it).
- [ ] **Step 3 (GREEN + flips pinned):** Step 1's test passes. Then collapse the characterization pair EXPLICITLY as follows: DELETE `test_remote_adoption_uncataloged_dest_twin_current_behavior` (its divergence pin is obsolete; cross-path equality is now the parity test's job), KEEP `test_local_adoption_uncataloged_dest_twin_current_behavior` as the adoption positive control (the meta-test comment at 9647-9648 already points at it) with its `"ok"` pin and a docstring update removing the cross-reference to the deleted twin; move `_ADOPTION_SCENARIO` back into `_BEHAVIOR_PARITY_SCENARIOS`, delete the DIVERGES comment, update both meta-tests; run `test_remote_import_invalidates_raw_caches_when_adopted_jpeg_pairs` FIRST (its flow moved; must pass unchanged); run the dest_read_cancel 2x2 (the remote-adoption test must still pass — adoption now feeds the `landed` guard term; rewrite its docstring/comments and the remote-fresh test's guard-text references); Task 3's rollback pins must still pass (single decrement). Write two SMALL new pins for flips 2 and 3: (a) extend the adopted-jpeg WC coverage: in an adoption-geometry test with `vireo_dir`, assert `wc_source_paths` behavior via the `_extract_working_copies` spy — the adopted mount path now carries a card-side source override (mirror the local behavior); (b) a remote adopted-validation-failure test asserting the NEW `unsafe_files[].path` (mount dest path) and reason wording — if one exists pinning the OLD card-path subject, update it with a flip-3 comment instead of adding one.
- [ ] **Step 4:** Docstring/comment sweeps: the ~9 test blocks and ~12 production comments listed in Context that reference `adopted_paths`/the old guard. Grep `adopted_paths` at the end — remaining hits must be zero in production and only historical mentions in tests/docs if any are genuinely historical.
- [ ] **Step 5:** Full file (timeout 600000) → expect ~220-221 passed, 1 skipped (arithmetic: 220 after Task 3; scenario promotion adds ZERO collected tests — the parity tests are loops, not parametrized; deleting the remote characterization test −1; flip pins +1-2 — recount honestly and report the exact number). Commit: `"Fold remote adoptions into landed: hash-status stamp, WC override, unified failure reporting (spec divergence 10 + PR 5a flips)"`

### Task 6: Full verification + PR

- [ ] **Step 1:** Full file; exact count from Task 5.
- [ ] **Step 2:** Required suite (CLAUDE.md list, timeout 600000; known env failure `test_api_exiftool_status_reports_missing` ignored).
- [ ] **Step 3:** Push; `gh pr create --base main --title "Import unification PR 5a: unify the landed ledgers and fold remote adoptions (spec divergence 10)" --body "<BODY>"`. Body: spec/plan links; the 5a/5b split rationale; the THREE flips with their pins; the double-rollback hazard and its Task-3 pins; the dead-counter deletion; the zero-byte dropped-re-read note; the net line delta (expect roughly −200 production); notes for PR 5b (the `verified_counted_for_copies` flag and `_reclassify_landed_failed` twins are the seams `_ImportRunState`/the transport's `attests_bytes` will absorb); preflight mirrors unaffected (decision walk untouched; bookkeeping only); exact test counts. End with the Claude Code attribution line.
