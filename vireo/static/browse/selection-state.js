/* Selection membership and focus have one owner. Callers receive a read-only
   live view or copied ids; they cannot mutate the backing Set. */
class SelectionIds {
  /** @type {() => Set<number>} */
  #read;
  /** @param {() => Set<number>} read */
  constructor(read) { this.#read = read; Object.freeze(this); }
  get size() { return this.#read().size; }
  /** @param {number} id */
  has(id) { return this.#read().has(id); }
  /** @param {(id: number) => void} visit */
  forEach(visit) { this.#read().forEach(id => visit(id)); }
  [Symbol.iterator]() { return this.#read().values(); }
}

class BrowseSelection {
  #ids = new Set(/** @type {number[]} */ ([]));
  #focusedPhotoId = /** @type {number | null} */ (null);
  #anchorIndex = -1;
  #lastClickedPhotoId = /** @type {number | null} */ (null);
  #beforeClick = /** @type {Set<number> | null} */ (null);
  #owner = {};
  #view = new SelectionIds(() => this.#ids);

  get batchIds() { return this.#view; }
  get focusedPhotoId() { return this.#focusedPhotoId; }
  get anchorIndex() { return this.#anchorIndex; }
  get lastClickedPhotoId() { return this.#lastClickedPhotoId; }
  ids() { return this.#ids.size ? Array.from(this.#ids) : this.#focusedPhotoId == null ? [] : [this.#focusedPhotoId]; }
  invalidate() { this.#owner = {}; }
  observe() { const owner = this.#owner; return () => this.#owner === owner; }

  /** @param {number | null} id @param {number} index */
  focus(id, index = this.#anchorIndex) {
    this.#focusedPhotoId = id;
    this.#anchorIndex = index;
    this.invalidate();
  }
  /** @param {number} index */
  reanchor(index) { this.#anchorIndex = index; }
  /** @param {number} id */
  rememberClick(id) { this.#lastClickedPhotoId = id; }
  captureClick() { this.#beforeClick = new Set(this.#ids); }
  takeClickSnapshot() { const ids = this.#beforeClick; this.#beforeClick = null; return ids; }
  /** @param {Iterable<number>} ids */
  replaceBatch(ids) { this.#ids = new Set(ids); this.invalidate(); }
  /** @param {number} id */
  addToBatch(id) { this.#ids.add(id); this.invalidate(); }
  /** @param {number} id */
  removeFromBatch(id) { const removed = this.#ids.delete(id); if (removed) this.invalidate(); return removed; }
  clearBatch() { this.#ids.clear(); this.invalidate(); }
  clear() { this.clearBatch(); this.focus(null, -1); }
  /** @param {number} id @param {number} index */
  selectSingle(id, index) { this.clearBatch(); this.focus(id, index); }
  /** @param {Iterable<number>} ids @param {number} index */
  selectBatch(ids, index) { this.replaceBatch(ids); this.focus(null, index); }
  /** @param {Iterable<number>} ids @param {Iterable<number>} targets */
  extendRange(ids, targets) {
    for (const id of ids) this.#ids.add(id);
    if (this.#focusedPhotoId != null) this.#ids.add(this.#focusedPhotoId);
    for (const id of targets) this.#ids.add(id);
    this.invalidate();
  }
  /** @param {readonly number[]} ids */
  toggle(ids) {
    if (!this.#ids.size && this.#focusedPhotoId !== null) this.#ids.add(this.#focusedPhotoId);
    const remove = ids.every(id => this.#ids.has(id));
    ids.forEach(id => { if (remove) this.#ids.delete(id); else this.#ids.add(id); });
    const droppedFocus = this.#focusedPhotoId !== null && !this.#ids.has(this.#focusedPhotoId)
      && (this.#ids.size > 0 || ids.includes(this.#focusedPhotoId));
    if (droppedFocus) this.focus(null, -1);
    this.invalidate();
    return droppedFocus;
  }
  beginSelectAll() { this.focus(null, -1); return this.observe(); }
}

export function createBrowseSelection() { return Object.freeze(new BrowseSelection()); }
export const selection = createBrowseSelection();
