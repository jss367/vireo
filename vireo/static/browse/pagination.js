/* Pagination metadata and the loading mutex belong together. A completion
   releases only the load that created it, even after a reset or newer load. */
class BrowsePagination {
  #first = 1;
  #next = 1;
  #pageSize = 50;
  #allLoaded = false;
  #ready = false;
  #total = 0;
  #underlyingTotal = 0;
  #stackCount = 0;
  #load = /** @type {object | null} */ (null);

  get earliestPage() { return this.#first; }
  get currentPage() { return this.#next; }
  get perPage() { return this.#pageSize; }
  get allLoaded() { return this.#allLoaded; }
  get ready() { return this.#ready; }
  get total() { return this.#total; }
  get underlyingTotal() { return this.#underlyingTotal; }
  get stackCount() { return this.#stackCount; }
  get loading() { return this.#load !== null; }
  get offset() { return (this.#first - 1) * this.#pageSize; }

  /** @param {number} size */
  configure(size) { if (Number.isInteger(size) && size > 0) this.#pageSize = size; }
  /** @param {BrowseTotals | null} data */
  setTotals(data) {
    this.#total = Number(data && data.total) || 0;
    this.#stackCount = Number(data && data.stack_count) || 0;
    this.#underlyingTotal = data && data.underlying_total != null
      ? Number(data.underlying_total) || 0 : this.#total;
  }
  /** @param {number} first @param {number} next @param {number} count */
  place(first, next, count) {
    this.#first = first;
    this.#next = next;
    this.#allLoaded = this.offset + count >= this.#total;
    this.#ready = true;
  }
  /** @param {number} count @param {number} chunk @param {number | null} focusedPage @param {boolean} empty */
  append(count, chunk, focusedPage = null, empty = false) {
    if (focusedPage !== null) this.#first = this.#next = focusedPage;
    this.place(this.#first, this.#next + (empty ? 0 : chunk), count);
    if (empty) this.#allLoaded = true;
  }
  /** @param {number} first @param {number} count */
  prepend(first, count) { this.place(first, this.#next, count); }
  /** @param {number} count */
  removePhotos(count) {
    this.#total = Math.max(0, this.#total - count);
    this.#underlyingTotal = Math.max(0, this.#underlyingTotal - count);
  }
  reset() { this.#first = this.#next = 1; this.#allLoaded = this.#ready = false; }
  cancelLoad() { this.#load = null; }
  beginLoad() {
    const load = {};
    this.#load = load;
    return () => {
      if (this.#load !== load) return false;
      this.#load = null;
      return true;
    };
  }
}

export function createBrowsePagination() { return Object.freeze(new BrowsePagination()); }
export const pagination = createBrowsePagination();
