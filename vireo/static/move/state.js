// Move page state: folders, the photo grid and its selection, the folder
// browser, recent destinations, the confirm callback, the rule editor, remote
// targets, the Quick Move preflight and the keep-visible check. ES module: the
// page's other modules import `state` and read and write its fields; nothing
// here is a global.

/**
 * @typedef {object} MoveState
 * @property {MoveFolder[]} allFolders
 * @property {MovePhoto[]} photoList
 * @property {Set<number>} selectedPhotos
 * @property {number} lastSelectedIdx
 * @property {number} photoLoadSeq
 *   Bumped by every folder's photo load; a slower load for an earlier folder
 *   is dropped.
 * @property {string | null} browserTargetInput
 *   The input the folder browser fills in.
 * @property {string} browserPath
 * @property {string} browserHomePath
 * @property {number} browseSeq
 *   Bumped by every browseTo(); a slower listing is dropped.
 * @property {string[]} recentDestinations
 * @property {number} recentDestinationsSeq
 * @property {(() => unknown) | null} confirmCallback
 * @property {number | null} editingRuleId
 * @property {boolean} quickMoveBusy
 * @property {MoveRemoteTarget[]} quickRemoteTargets
 * @property {boolean} quickRsyncAvailable
 * @property {number} quickPreflightSeq
 * @property {ReturnType<typeof setTimeout> | null} quickPreflightTimer
 * @property {number} moveVisibilityRequest
 * @property {boolean} moveVisibilityReady
 * @property {MoveRule[]} rulesList
 */

/** @type {MoveState} */
export const state = {
  allFolders: [],
  photoList: [],
  selectedPhotos: new Set(),
  lastSelectedIdx: -1,
  photoLoadSeq: 0,
  browserTargetInput: null,
  browserPath: '',
  browserHomePath: '',
  browseSeq: 0,
  recentDestinations: [],
  recentDestinationsSeq: 0,
  confirmCallback: null,
  editingRuleId: null,
  quickMoveBusy: false,
  quickRemoteTargets: [],
  quickRsyncAvailable: false,
  quickPreflightSeq: 0,
  quickPreflightTimer: null,
  moveVisibilityRequest: 0,
  moveVisibilityReady: false,
  rulesList: [],
};
