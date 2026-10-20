// The Best Batch page (vireo/static/best-batch/): the ranked batch from
// /api/photos/best-batch and the shared globals its shared.js reads.
interface BestBatchCard {
  id: number; filename?: string; rank: number | null; role?: string;
  quality_pct?: number | null; focus?: number | null; focus_basis?: string;
  focus_evidence?: {measurement: number; cohort_size: number; absolute_status: string} | null;
  sharpness?: number | null; eye_x?: number | null; eye_y?: number | null;
  crop_url?: string | null; focus_crop_url?: string | null; evidence_status?: string;
  eye_status?: string | null; analysis?: {width: number; height: number} | null;
  subject?: {detection_id: number | null; selection: string; candidates: {
    detection_id: number; box: {x: number; y: number; w: number; h: number};
    predictions: {species: string; status: string}[];
  }[]};
  width?: number | null; height?: number | null; reasons?: string[];
}
interface BestBatchResponse {
  cards?: BestBatchCard[]; best_photo_id?: number | null; count: number;
  ranked_candidate_id?: number | null; ranking_status?: string; keep_review_message?: string;
  scored?: boolean; unscored_reason?: string | null;
  comparison?: {requested_species: string | null; subject_overrides: Record<string, number>};
  sequence_range?: [number, number] | null; photo_ids?: number[];
  suggested_reject_ids?: number[]; scope_method?: string;
}
interface Window {
  escapeHtml(value: unknown): string;
  escapeAttr(value: unknown): string;
  showToast?(message: string, kind?: string): void;
  vireoPreviewUrl(photoId: number, size: number): string;
  vireoThumbnailUrl?(photo: BestBatchCard | number): string;
  openLightbox(id: number, filename: string, photos: {id: number; filename: string}[]): void;
}
