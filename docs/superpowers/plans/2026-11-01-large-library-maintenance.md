# Predictable maintenance for large photo libraries

Date: November 1, 2026

Status: First change implemented and locally validated

Vireo should open and remain responsive as photo libraries grow. Start by removing the automatic thumbnail scan and moving preview reconciliation and eviction off the launch path. Add resumable auditing or broader cache accounting only when measurements justify that machinery.

Use a million-photo catalog as a scaling checkpoint. The architecture must also support the tens of millions of photos described in [Vireo's core philosophy](../../../CORE_PHILOSOPHY.md). A million-row synthetic database alone does not establish filesystem or network-storage performance.

## Current behavior and evidence

These observations describe the behavior before the first change. Implementation began after fast-forwarding the existing branch to `origin/main`.

| Area | Current behavior | Consequence |
| --- | --- | --- |
| Thumbnail audit | `thumb_path_backfill_candidate_count` in `vireo/thumbnails.py` loads all thumbnail rows and checks each expected file. Repair scans them again and calls the candidate count again afterward. | Even discovering that no repair is needed costs a full-library pass. |
| Thumbnail scheduling | `StartupTasks.kickoff_thumb_path_backfill` runs on a timer six seconds after launch and performs the candidate scan before creating the repair job. | It does not block app construction. It adds background work even when no repair is needed. |
| Thumbnail state updates | Generation records `thumb_path`, request-time regeneration persists it, and render-cache invalidation clears it. | Normal state maintenance already exists; removing the timer does not require rebuilding those paths. |
| Preview reconciliation | `reconcile_preview_cache` in `vireo/preview_cache.py` is called during app construction. It materializes preview entries, checks file existence, and enumerates untracked paired previews. | Startup performs work proportional to the preview cache. |
| Preview inventory | `vireo/repositories/caches.py` already stores preview sizes and access times. Eviction combines and sorts ordinary and paired preview entries. | Reuse this accounting, but replace unbounded candidate retrieval. |
| Generated-file cleanup | `vireo/services/retained_cache.py` walks cache directories and sorts candidates. `StartupTasks` schedules cleanup shortly after launch and hourly afterward. | Repeated discovery can be expensive even when little is deleted. |
| Directory listing cache | `vireo/dir_listing_cache.py` caches listings in memory with age and directory-count limits. | Restart loses observations, and directory count does not bound stored filenames or memory. |

A review supplied by the user reports 206,561 thumbnail rows checked in about 1.7 seconds with zero drift, and preview reconciliation over 44,613 rows in well under a second. These are reported live-catalog measurements, not reproduced results from this workspace. They support starting small; warm-cache timings do not establish behavior on cold or unavailable storage.

## Design constraints

- Keep the first change small. If measurements justify bounded background passes, bound discovery and reporting as well as processing; batching updates after loading every row is insufficient.
- Keep maintenance catalog-wide when its records are shared. Capture workspace scope explicitly for workspace-specific work; changing the selected workspace must not redirect a running task.
- If durable cursors are introduced later, persist progress only after the corresponding work is safely committed. Repeating a batch after a crash must be safe.
- Separate versioned one-time migrations from recurring audits. A migration marker cannot prove that cached files still exist.
- Treat missing files, inaccessible storage, and failed checks differently. An unavailable drive must not be interpreted as proof of deletion.
- Preserve source verification, file ownership, publication locks, and protections against concurrent cache use. An inventory row alone never authorizes deletion.
- Reuse the job runner, background I/O coordination, and maintenance history. Follow the [existing scheduling design](../specs/2026-08-15-resource-aware-job-scheduling-design.md) where applicable; a new scheduler is not a prerequisite.
- Report work actually checked and its freshness. Do not perform a full scan just to obtain an exact progress denominator or a count of remaining repairs.

## First change Remove unnecessary startup maintenance

Deliver two focused behavior changes in one implementation pull request, using the existing job runner. Do not add a general audit framework, durable cursors, or retry tables in this change.

### Remove automatic thumbnail scanning

1. Remove the startup timer and its full-library candidate check. Keep `backfill_thumb_paths` available through an explicit **Recheck thumbnails** action. The current call-site search shows the automatic launcher, not an existing user-facing recheck action; include the minimal launcher and control needed to make repair accessible.
2. Launch explicit repair without a preliminary full candidate scan. Remove the trailing full recount. Report checked, corrected, and unchecked records from the pass itself; unchecked records are not a known count of required repairs. Do not report zero remaining drift after cancellation or concurrent changes without evidence.
3. Preserve generation, invalidation, deletion, and request-time repair behavior. Add focused regression coverage for the paths affected by timer removal instead of redesigning state tracking.
4. Keep explicit repair available for older catalogs, external deletion, and failed state writes. Zero drift in one catalog does not prove every user's legacy migration is finished. Request-time repair updates accessed photos; unvisited records may remain stale until an explicit recheck. Make the dashboard count's recorded-state meaning clear without claiming a current disk audit.
5. Preserve the distinction between an inaccessible cache root and a missing file. Explicit repair must not clear recorded paths merely because storage is unavailable.

### Move preview maintenance into a background system job

1. Remove synchronous preview reconciliation and eviction from app construction. Schedule a visible catalog-wide singleton job after the runner is ready, with its own database connection and normal teardown handling.
2. Keep reconciliation before eviction and preserve quota accounting, missing-file cleanup, rejection of unregistered paired cache hits, and the writer-lock recheck before orphan deletion.
3. Check the new concurrency boundary: requests and publishers can now run during reconciliation. In particular, a missing-file snapshot must not delete a row that was republished before cleanup commits. Add the smallest locking or conditional-update correction needed and test overlapping eviction as well.
4. Use existing background I/O coordination and accurate job history/error reporting. A background pass can initially remain linear if measured contention and memory are acceptable. Do not claim it is bounded or resumable simply because it runs in a job.
5. Preserve quota enforcement on normal publication paths. Startup can temporarily serve requests before reconciliation finishes; explain that behavior in the implementation description.

### Verification for the first change

- Instrument app construction with small and large catalogs. The thumbnail and preview maintenance paths perform zero per-photo or per-preview checks before construction returns. Advance the startup timers in a controlled test to verify that the thumbnail scan does not merely run later.
- Hold the preview job at a synchronization point and verify that the app can serve requests; release it and verify reconciliation runs before eviction. Check singleton behavior, errors, and shutdown.
- Explicit thumbnail recheck repairs missing and unrecorded paths in one pass without a pre-scan or trailing recount. Cancellation reports incomplete work honestly, and unavailable storage preserves records.
- Concurrent preview publication does not lose a newly valid row or file. Failed deletion retains accounting. Existing on-demand thumbnail repair still updates recorded state.
- Measure launch, browsing latency, and memory with preview maintenance enabled and disabled. Record the remaining synchronous startup work, including working-copy quota enforcement, separately. These two cuts remove specific scans; they do not prove all startup work is independent of library size.

Use targeted tests for these behaviors and the repository's impact-selected test workflow. Run no more than four local pytest workers. Operation-count and synchronization tests establish the behavior; machine-sensitive timing assertions are not substitutes.

## Measurement gate for further auditing

After the first change, measure background preview reconciliation, hourly cleanup, and folder checks under representative browsing and import activity. Prioritize slow or unavailable network storage, since a short warm-cache scan does not describe network stalls. Pursue the next change where measurements show user-visible contention, excessive memory, or repeated lost work after interruption.

A general resumable audit is deferred. If needed, use indexed keyset pages, a versioned cursor scoped to the catalog and cache, an upper pass boundary, and atomic work/progress commits. Retain failed checks for retry without blocking unrelated reachable work. Continuous imports must not prevent a pass from finishing, and reporting must not trigger another full scan.

For preview reconciliation, page existing inventory records and separate tracked-file checks from orphan discovery. Avoid combined in-memory lists, whole-directory glob results, and unbounded filename sets. Filesystem enumeration has no portable durable cursor: choose bounded directory sharding or persistent incremental discovery before promising restartable orphan sweeps. Test one oversized directory as well as many small directories.

Introduce this machinery only if simpler removal, deferral, or bounded queries do not meet the measured need. Do not add periodic thumbnail audits merely to replace the timer removed in the first change.

## Follow-up option Bounded cleanup and broader cache accounting

Extend existing inventory coverage one cache family at a time. Record ownership, artifact identity, size, and access information when files are published or invalidated. Reconcile gaps left by crashes and external changes incrementally.

Query indexed batches of eligible entries. Maintain recoverable byte accounting so deciding whether cleanup is needed does not require repeatedly summing every row. Coalesce access-time updates to avoid turning browsing into a stream of database writes.

Bound discovery and source verification as well as deletion. Hashing original files needs byte-level checkpoints; a file-count budget alone is insufficient. Failed deletion retains accounting and retry state. Bootstrap an inventory in the background without trusting incomplete inventory as proof that disk usage is below quota.

Acceptance: a normal cleanup cycle does not enumerate or sort the entire generated-file tree, memory stays bounded, and interruption or concurrent publication preserves existing safety guarantees.

## Follow-up option Adaptive folder checks

Persist compact directory observations with volume identity and freshness. Treat filesystem notifications as hints and periodically reconcile to catch missed events. Prioritize recently changed and active folders, back off inaccessible volumes, and bound cached listing memory by bytes or entry count as well as directory count.

Preserve the current protections for recent modifications, stale network listings, remounted volumes, and explicit rechecks. Persisted timestamps must remain meaningful across restarts; process-local monotonic values cannot be reused as durable timestamps.

## Measurement and rollout

Measure launch to usable browsing, request latency at the 95th percentile, process memory, database write-lock duration, rows read, filesystem operations, bytes read, and audit completion rate. Compare maintenance enabled and disabled on the same workload.

Use catalogs of 10,000, 100,000, and 1,000,000 photos with unchanged data, sparse repairs, and heavy repair demand. Pair synthetic database tests with representative real cache files and local storage, a slow network share, and an unavailable external drive. Include concurrent browsing/import, repeated restarts, continuous activity, and one very large directory.

For the first change, acceptance means the targeted scans leave app construction, the thumbnail timer is removed, repair remains accessible, and concurrent preview maintenance is safe. For later bounded passes, also require bounded memory and safe recovery. Set absolute latency and resource thresholds after collecting the baseline on named hardware.

Maintenance should yield to interactive work while receiving enough bounded service to eventually finish on reachable storage. A cooperative deadline cannot interrupt a stalled filesystem call. If measurements expose this problem, isolate or otherwise bound blocking volume operations before promising a hard response-time limit; avoid accumulating abandoned worker threads.

Ship each justified change separately with its measurements and regression checks. If background maintenance must be disabled after a regression, keep on-demand repair available and retain any durable progress that exists. Do not restore unconditional full-library startup scans as a fallback.

## Implementation and validation

The first change removes the thumbnail startup timer and both redundant candidate scans. Storage now provides **Recheck thumbnails**; `POST /api/thumbnail-cache/recheck` returns HTTP 202 with `job_id` and `joined`. It starts or joins a catalog-wide user job. Its result distinguishes `checked`, `set`, `cleared`, and `unchecked` records without claiming an exact remaining repair count.

Preview reconciliation and eviction now run in a catalog-wide system job with cancellation, transfer priority, and normal shutdown handling. Publication and cleanup coordinate through the catalog writer transaction; cleanup rechecks snapshot candidates before changing files or accounting. Background failures appear in job history, and no-op system runs are dropped.

Validation covers forbidden inventory reads during app construction at two catalog sizes, absence of a thumbnail timer, explicit repair and history, unavailable cache storage, cancellation, concurrent publication, overlapping eviction, and browser interaction with the new Storage control. The fallback regression suite, focused follow-up suites, backend type checking, frontend checks, and browser test were run. These tests establish the targeted behavior; they are not a million-photo or network-storage performance benchmark.

## Next measurement

Measure background preview reconciliation, hourly cleanup, and folder checks during browsing and imports, especially on slow or unavailable network storage. Use the results to choose the next improvement. Background passes remain linear and the durable audit framework remains deferred. Other synchronous startup work, including migrations and working-copy maintenance, remains outside this first change.
