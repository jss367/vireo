// The Best Batch page (vireo/static/best-batch/): the ranked batch from
// /api/photos/best-batch and the shared globals its shared.js reads.
interface BestBatchCard {
  id: number; filename?: string; rank: number | null; role?: string;
  quality_pct?: number | null; focus?: number | null; focus_basis?: string;
  focus_evidence?: {measurement: number; cohort_size: number; absolute_status: string} | null;
  sharpness?: number | null; eye_x?: number | null; eye_y?: number | null;
  width?: number | null; height?: number | null; reasons?: string[];
}
interface BestBatchResponse {
  cards?: BestBatchCard[]; best_photo_id?: number | null; count: number;
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
