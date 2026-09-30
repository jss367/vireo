// Shared source, preview, selection, readiness, and job state.
// Classic page script; load boot.js after all definitions.

/* Import page: adds folders in place or copies them to an archive, renders
   live progress from the job's SSE stream, and only shows safe-to-format
   messaging for verified archive-copy imports. */
let sources = [];
let activeJobId = null;
let es = null;
// Card folders the finished import copied from — handed to the Card
// cleanup page by the result card's "Free up card space…" button.
let cardCleanupImportSources = [];
let stagingResultsByPath = {};
let sourceCounts = {};
let sourceCountTickTimer = null;
let activeImportMode = 'in_place';
let importRemoteTargets = [];
let importRsyncAvailable = false;
let importSshAvailable = false;
let destStructureSeq = 0;
// Monotonic sequence for the import preview / duplicate-check flow so a
// slow older previewImport() response can't overwrite the summary text
// or trigger a folder-structure render on top of a newer preview's
// results. Incremented at the top of previewImport().
let importPreviewSeq = 0;
let importPreviewTimer = null;
// Own every request spawned by the active preview. Starting a new preview
// aborts this controller so an obsolete byte-for-byte duplicate check does
// not keep reading the card / archive in the background.
let importPreviewAbort = null;
let importThumbSchedulerCancel = null;
// Last args passed to renderImportPreviewGrid, so the "Hide duplicates"
// filter can re-render locally.
let lastImportPreviewRender = null;
let importTags = [];
// User intent ONLY: paths the user explicitly unchecked. Duplicate verdicts
// are a separate eligibility overlay and are never written in here — see the
// import-file-selection spec §1. Keeping these separate is what makes the
// checkbox state safe to re-derive on every render.
let importDeselected = new Set();
let importPreviewedPaths = [];
let importSelectionAnchor = null;
let importDayGroups = new Map();
let importCollapsedDays = new Set();
let newImagesSnapshotId = null;
// Preview lifecycle, read only by updateStartGate(). The four states below
// must stay distinguishable — collapsing "no preview run" with "in flight"
// is what would let a user who picked 100 of 5,000 files hit Start during the
// disk walk and copy all 5,000, because previewImport() clears the grid
// BEFORE the walk and the signature at that moment still matches the UI.
//   null captured signature .. no preview has completed; Start imports
//                              everything, which is the pre-selection
//                              behaviour and is safe because nothing on
//                              screen claims otherwise.
//   captured + unchanged .... the grid on screen is what will be imported.
//   captured + changed ...... the controls moved on; the grid is a lie.
//   in flight / draining .... the grid is empty or incomplete.
let importPreviewInFlight = false;
let importDupStreamPending = false;
let importPreviewFailed = false;
let importPreviewCapturedSignature = null;
// True between POSTing the import job and learning its id — activeJobId is
// still null across that await, and without this the gate would re-enable
// Start and allow a double submit.
let importStartPending = false;
// Owned by the new-images (snapshot) flow: Start is blocked while the frozen
// list loads, when the capture found nothing, and after a load failure. It
// contributes no label — #newImagesImportSource already says why.
let newImagesStartBlocked = false;
let importExiftoolReady = null;
let importExiftoolRequired = true;
let lastFinishedImportJob = null;
