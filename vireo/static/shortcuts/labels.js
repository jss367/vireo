// The editable shortcuts: each context's title and action labels, and the
// default binding of every action.
// ES module.

/** @type {Record<string, Record<string, string>>} */
export const SHORTCUT_LABELS = {
  navigation: {
    _title: 'Navigation',
    import: 'Go to Import',
    pipeline: 'Go to Process',
    pipeline_review: 'Go to Process Review',
    review: 'Go to Review',
    cull: 'Go to Cull',
    browse: 'Go to Browse',
    map: 'Go to Map',
    dashboard: 'Go to Dashboard',
    storage: 'Go to Storage',
    audit: 'Go to Audit',
    id_conflicts: 'Go to ID Conflicts',
    workspace: 'Go to Workspace',
    shortcuts: 'Go to Shortcuts',
    settings: 'Go to Settings',
    keywords: 'Go to Keywords',
    lightroom: 'Go to Lightroom',
  },
  review: {
    _title: 'Review (Classify page)',
    accept: 'Accept prediction',
    reject: 'Reject prediction',
    skip_pending: 'Skip for now (keep pending)',
  },
  pipeline_rapid_review: {
    _title: 'Rapid Review',
    pick: 'Pick photo',
    reject: 'Reject photo',
    forward: 'Next photo', previous: 'Previous photo',
    leave: 'Leave for review and advance', undo: 'Undo last decision',
    clear: 'Clear to review',
    apply: 'Apply staged changes',
    exit: 'Exit to classic review',
    zoom: 'Toggle zoom',
  },
  browse: {
    _title: 'Photo shortcuts',
    rate_0: 'Rate 0 (clear)',
    rate_1: 'Rate 1',
    rate_2: 'Rate 2',
    rate_3: 'Rate 3',
    rate_4: 'Rate 4',
    rate_5: 'Rate 5',
    flag: 'Pick photo (Browse, Lightbox, Misses, Rapid Review)',
    reject: 'Reject photo (Browse, Lightbox, Misses, Rapid Review)',
    unflag: 'Clear flag (Browse, Lightbox, Misses, Rapid Review)',
    undo: 'Undo last action',
    redo: 'Redo last undo',
    select_all: 'Select all photos',
    compare: 'Compare selected photos',
    zoom: 'Toggle zoom (Lightbox, Rapid Review)',
    toggle_boxes: 'Toggle detection boxes (lightbox)',
    toggle_ui: 'Toggle UI controls (lightbox)',
    color_red: 'Color label: Red',
    color_yellow: 'Color label: Yellow',
    color_green: 'Color label: Green',
    color_blue: 'Color label: Blue',
  },
};

/** @type {Record<string,Record<string,string | undefined>>} */
export const SHORTCUT_DEFAULTS = {
  navigation: {
    import: '', pipeline: '', pipeline_review: '', review: '',
    cull: '', browse: '', map: '',
    dashboard: '', storage: '', audit: '', id_conflicts: '', workspace: '',
    shortcuts: '', settings: '', keywords: '', lightroom: '',
  },
  review: { accept: 'a', reject: 'x', skip_pending: 's' },
  pipeline_rapid_review: {
    forward: 'arrowright', previous: 'arrowleft', leave: 's', undo: 'ctrl+z',
    apply: 'ctrl+enter', exit: 'escape',
  },
  browse: {
    rate_0: '0', rate_1: '1', rate_2: '2', rate_3: '3',
    rate_4: '4', rate_5: '5',
    flag: 'p', reject: 'x', unflag: 'u',
    undo: 'ctrl+z', redo: 'ctrl+shift+z', select_all: 'ctrl+a', compare: 'c', zoom: 'z',
    toggle_boxes: 'b', toggle_ui: 'h',
    color_red: '6', color_yellow: '7', color_green: '8', color_blue: '9',
  },
};

/** A context's default bindings, copied so edits never touch the defaults. @param {string} ctx */
export function defaultsFor(ctx) {
  return /** @type {Record<string, string>} */ (JSON.parse(JSON.stringify(SHORTCUT_DEFAULTS[ctx] || {})));
}

/** Rapid Review inherits these photo actions unless a legacy customization overrides one.
 * @type {Record<string,string>} */
export const RAPID_SHARED = {pick: 'flag', reject: 'reject', clear: 'unflag', zoom: 'zoom'};

/** @param {Record<string,Record<string,string | undefined>>} shortcuts */
export function effectiveRapidBindings(shortcuts) {
  const rapid = {...SHORTCUT_DEFAULTS.pipeline_rapid_review};
  for (const [action, shared] of Object.entries(RAPID_SHARED)) {
    rapid[action] = shortcuts.browse?.[shared] ?? SHORTCUT_DEFAULTS.browse?.[shared];
  }
  return {...rapid, ...shortcuts.pipeline_rapid_review};
}
