// The ID Conflicts page (vireo/static/id-conflicts/): the comparison rows
// from /api/predictions/compare and the shared globals its classic scripts call.
interface CompareChip {id: string; label: string}
interface CompareStatus {category?: string; label?: string}
interface ComparePrediction {
  id: number; species: string; confidence?: number | null; status?: string; category?: string;
  category_label?: string; category_detail?: string; effective_category?: string;
  matched_keyword?: string; shared_rank?: string;
}
interface CompareSubject {
  signal_consensus_species?: string; box?: {x?: number; y?: number; w?: number; h?: number} | null;
  predictions?: Record<string, ComparePrediction[] | undefined>;
}
interface CompareKeyword {name: string; type?: string; is_species?: boolean; taxon_rank?: string | null}
interface CompareSignal {
  additional_subject_count?: number; model_disagreement_score?: number; model_disagreement_label?: string;
  keyword_conflict_count?: number; max_keyword_conflict_confidence?: number; consensus_count?: number;
  consensus_label?: string; missing_visible_model_count?: number;
}
interface ComparePhoto {
  photo_id: number; filename?: string; status?: CompareStatus; subjects?: CompareSubject[];
  subject_statuses?: CompareStatus[]; signal?: CompareSignal; keywords?: CompareKeyword[];
  timestamp?: string; rating?: number; flag?: string; wildlife_excluded?: boolean;
  miss_no_subject?: boolean; miss_clipped?: boolean; miss_oof?: boolean; best_prediction_id?: number | null;
}
interface CompareResponse {
  photos?: ComparePhoto[]; total?: number; page?: number; per_page?: number; token?: string;
  models?: string[]; visible_models?: string[]; filters?: CompareChip[]; sorts?: CompareChip[];
  excludes?: CompareChip[]; summary?: Record<string, number | undefined>;
  filter_counts?: Record<string, number | undefined>; exclusion_counts?: Record<string, number | undefined>;
  taxonomy_available?: boolean;
}
/** jsonFetch's rejection: whether the server answered, and its status. */
interface CompareRequestError extends Error {status?: number; answered?: boolean}
/** What a row request asks for (fetchRows / buildRowsUrl). */
interface CompareRowsOptions {
  page?: number; control?: boolean; initial?: boolean; afterDecision?: boolean;
  filterResolved?: boolean; refreshPhotoIds?: number[]; internal?: boolean;
}
interface CompareScope {collectionId: string; loadingSeq: number}
// The shared globals the page's shared.js reads.
interface Window {
  Vireo: VireoPageHookRegistry & {predictions: {groupedDecisionTracker(): {
    alreadyDecided(predictionId: number): boolean; record(decision: unknown): void;
  }}};
  VireoTextSearch: {
    readOptions(namespace: string): {matchCase?: boolean; wholeWord?: boolean};
    appendParams(params: URLSearchParams, options: {matchCase?: boolean; wholeWord?: boolean}): void;
    toggle(namespace: string, option: string, callback: () => unknown): void;
  };
  VireoViewPreferences: {read(key: string): string | null; write(key: string, value: string): void};
  escapeHtml(value: unknown): string;
  escapeAttr(value: unknown): string;
  showToast?(message: string, kind?: string): void;
  openLightbox(id: number, filename: string, photos: {id: number; filename: string}[]): void;
  vireoThumbnailUrl?(photo: ComparePhoto): string;
}
