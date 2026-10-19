// Cull page state shared by the page's classic scripts: the analysis on
// screen, the busy and pending flags, the user's pinned and applied
// decisions, the collection scope, the expanded species, the focused group,
// the slider debounce timers and the slider defaults.
// Classic page script; loads first among the Cull page scripts.

/** @type {CullData | null} */
var cullData = null;
var cullBusy = false;
var cullAnalysisPending = 0;
var cullAnalysisSeq = 0;
var cullDirty = false;
// Per-photo decisions the user made by hand this session: photo_id -> action.
// They outrank both the saved flag and the freshly computed label, so a
// slider tweak re-runs the analysis without silently undoing your calls.
/** @type {Record<string, CullAction>} */
var cullManualDecisions = {};
// Pinned decisions already written to the database, photo_id -> action.
// "Review" is the one decision a photo flag cannot hold, so a review pin
// stays after Apply (otherwise the card would snap back to the suggestion
// the moment you saved it) — it just no longer counts as unsaved work.
/** @type {Record<string, CullAction>} */
var cullAppliedDecisions = {};
// Saved photo flags always outrank suggestions.
/** @type {CullSourceResults | null} */
var pipelineResults = null;
/** @type {number | null} */
var selectedCollectionId = null;
/** @type {Record<string, boolean>} */
var expandedSpecies = {};
/** @type {CullGroupRef} */
var focusedCullGroup = { speciesIdx: null, groupIdx: null };
/** @type {ReturnType<typeof setTimeout> | null} */
var _reflowTimer = null;
/** @type {ReturnType<typeof setTimeout> | null} */
var _regroupTimer = null;

/** @type {Record<string, number>} */
var SCORING_DEFAULTS = {};
/** @type {Record<string, number>} */
var GROUPING_DEFAULTS = {};
