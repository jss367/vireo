// The Highlights page (vireo/static/highlights/): the /api/highlights
// payloads it renders and the shared globals its shared.js reads.
interface HighlightsPhoto {
  id: number; filename: string; flag?: string; species?: string | null;
  highlight_score?: number | null; reasons?: string[];
  predicted_species?: string | null; predicted_confidence?: number | null;
  has_accepted_species?: boolean; is_confirmable_prediction?: boolean;
  is_species_representative?: boolean; is_highlighted?: boolean; highlight_rank?: number | null;
  timestamp?: string | null; is_analyzed?: boolean;
  [key: string]: unknown;
}
/** A species bucket or the unidentified section: the photos loaded so far. */
interface HighlightsSection {
  species?: string; photos: HighlightsPhoto[]; photo_count: number; has_more?: boolean; loaded_count?: number;
  best_score?: number | null; best_timestamp?: string | null;
  [key: string]: unknown;
}
interface HighlightsBucket extends HighlightsSection {
  species: string; highlight_count?: number; is_accepted?: boolean; avg_confidence?: number | null; certainty?: string;
}
/** A page of one bucket from /api/highlights/bucket. */
interface HighlightsBucketPage extends HighlightsSection {loaded_count: number; has_more: boolean}
interface HighlightsData {
  buckets?: HighlightsBucket[]; unidentified?: HighlightsSection | null;
  folders: {id: number; name: string; path?: string; photo_count?: number}[];
  meta: {eligible: number; total_in_scope: number; [key: string]: unknown};
  [key: string]: unknown;
}
interface HighlightsFlagChange {photoId?: number | null; flag?: string; previousFlag?: string}
interface HighlightsLightboxPhoto {id: number; filename?: string; flag?: string}
interface Window {
  escapeAttr(value: unknown): string;
  VireoTextSearch: {
    readOptions(namespace: string): Record<string, boolean | undefined>;
    appendParams(params: URLSearchParams, options: Record<string, boolean | undefined>, matchCaseKey?: string, wholeWordKey?: string): void;
    toggle(namespace: string, option: string, callback: () => unknown): void;
  };
  VireoViewPreferences: {restoreAll(root: HTMLElement): void};
  safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  showToast?(message: string, kind?: string): void;
  openLightbox(id: number, filename: string, photos: HighlightsLightboxPhoto[]): void;
  closeLightbox(): void;
  vireoLightboxSession?: {requestedPhotoId(): number | null};
  _lightboxPhotoList?: HighlightsLightboxPhoto[];
  vireoThumbnailUrl?(photo: HighlightsPhoto): string;
  isAdvancedMode?(): boolean;
  Vireo: VireoPageHookRegistry;
}
