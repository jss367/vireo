// Life List page state: the /api/life-list payload and its request sequence,
// per-species photo paging, the export column picks, the publish preflight and
// site export, the shared folder browser, and the explorer's drill, rank-view
// and leaf state. ES module: the page's other modules import `state` and read
// and write its fields; nothing here is a global.

/**
 * @typedef {object} LifeListState
 * @property {LifeData|null} currentData
 *   The /api/life-list payload, read by the filters, the cards and the
 *   lightbox listeners.
 * @property {number} lifeListLoadSeq
 *   Owned by loadLifeList(); a response for an older sequence is dropped.
 * @property {ReturnType<typeof setTimeout>|null} debounceTimer
 * @property {string|null} lifeListLightboxSpecies
 *   The species whose photos the lightbox is paging through.
 * @property {Record<string,Promise<LifePhoto[]>>} lifeListLoadPromises
 *   One in-flight "more photos" page per species.
 * @property {Record<'species'|'photos',Record<string,boolean>|null>} lifeListExportColumnSelections
 * @property {ReturnType<typeof setTimeout>|null} publishPreflightTimer
 * @property {number} publishPreflightSequence
 * @property {boolean} publishPreflightReady
 * @property {boolean} siteExportStarting
 * @property {VireoFolderBrowser|null} publishFolderBrowser
 *   The shared folder browser the publish and site-export pickers use, made
 *   by boot.js.
 * @property {LifeExplorer|null} explorerData
 *   Last payload from /api/life-list/explorer.
 * @property {LifeCrumb[]} explorerPath
 *   [{id,name,rank}] drill path from the class root down.
 * @property {LifeRankView|null} explorerRankView
 *   {rank,data,search,missingOnly,showAll} when the flat rank view is open.
 * @property {number} explorerViewReqId
 *   The explorer body's request token; a slower response is dropped.
 * @property {Record<number,LifeCrumb[]>} explorerSunburstLineage
 *   Node id -> root-to-node lineage.
 * @property {{species:LifeNode[]}|null} explorerLeafData
 *   Last species-leaf payload.
 * @property {LifeCrumb|null} explorerLeafGenus
 *   {id,name,rank} of the genus being shown.
 * @property {ReturnType<typeof setTimeout>|null} rankSearchTimer
 *   Debounce for the flat-view search input.
 * @property {boolean} explorerLoaded
 *   Lazy-load guard: the explorer is fetched once, on first open.
 */

/** @type {LifeListState} */
export const state = {
  currentData: null,
  lifeListLoadSeq: 0,
  debounceTimer: null,
  lifeListLightboxSpecies: null,
  lifeListLoadPromises: {},
  lifeListExportColumnSelections: {species: null, photos: null},
  publishPreflightTimer: null,
  publishPreflightSequence: 0,
  publishPreflightReady: false,
  siteExportStarting: false,
  publishFolderBrowser: null,
  explorerData: null,
  explorerPath: [],
  explorerRankView: null,
  explorerViewReqId: 0,
  explorerSunburstLineage: {},
  explorerLeafData: null,
  explorerLeafGenus: null,
  rankSearchTimer: null,
  explorerLoaded: false,
};

/** The shared folder browser; boot.js makes it before any picker can open. */
export function publishFolderBrowser() {
  if (!state.publishFolderBrowser) throw new Error('The Life List folder browser is not ready');
  return state.publishFolderBrowser;
}
