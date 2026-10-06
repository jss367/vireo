// Shared Jobs page state: job lists, the selection, and per-step UI state.
// Classic page script; load boot.js after all definitions.

var activeJobs = [];
var historyJobs = [];
var selectedJobId = null;
var selectedSource = null;
var pollTimer = null;
var sseSource = null;
var leafBuffers = {};
var leafBufferSources = {};
var LEAF_MAX = 20;
// Job ids the user has clicked Cancel on but the server hasn't yet
// moved out of the active list. Tracked here so subsequent renders
// (every 2s poll) keep showing the optimistic "Cancelling…" state
// instead of reverting to a fresh "Cancel" button.
var cancellingJobIds = {};
var collapsedSteps = {};
var activeWsId = null;
var workspaceNames = {};
var currentView = 'active'; // 'active', 'history', or a job id
