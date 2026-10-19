// The sync panel (_sync_panel.html, vireo/static/sync-panel/): pending XMP
// changes, the review dialog's progressive preview, and the sync job.
interface SyncProgress {
  current: number; total: number; current_file?: string; phase?: string;
  synced?: number; failed?: number; checkpoint?: number;
}
interface SyncJob {id: string; progress: SyncProgress | null}
interface SyncStatusSummary {
  pending_count: number; pending_photo_count: number;
  change_type_counts?: Record<string, number>; active_job?: SyncJob | null;
}
interface SyncChange {
  id: number; type: string; value?: string; old_value?: unknown;
  creates_xmp_sidecar?: boolean; paired_keyword_rename?: boolean; rating_requires_sidecar?: boolean;
  presentation?: SyncPresentation; presentation_with_sidecar?: SyncPresentation; presentation_without_sidecar?: SyncPresentation;
}
interface SyncPhoto {photo_id: number; filename: string; changes: SyncChange[]}
interface SyncPreviewData {
  photos: SyncPhoto[]; total_changes: number; total_photos: number;
  change_type_counts: Record<string, number>; location_sync_enabled?: boolean;
  location_keyword_sync_enabled?: boolean; revision?: string;
}
interface SyncPreviewPage extends SyncPreviewData {next_offset: number; has_more: boolean; revision: string}
interface SyncPresentation {field: string; action: string; before: string; after: string; before_detail?: string; after_detail?: string}
interface SyncGroupEntry {photo: SyncPhoto; change: SyncChange}
interface SyncChangeGroup {type: string; presentation: SyncPresentation; entries: SyncGroupEntry[]}
interface SyncJobEvent {
  status: string; errors?: string[]; failure?: {message?: string} | null;
  result?: {synced?: number; failed?: number; errors?: string[]} | null;
}
interface SyncRequestError extends Error {code?: string}

declare var Vireo: VireoPageHookRegistry;
declare var Keymap: {pushEsc(handler: () => void): number; popEsc(token: number | null | undefined): boolean; lockBodyScroll(): void; unlockBodyScroll(): void};
declare var VireoViewPreferences: {restore(input: HTMLInputElement): void; save?(input: HTMLInputElement): void};
declare var vireoLightboxSession: {requestedPhotoId(): number | null};
declare function safeFetch<T = unknown>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
declare function safeEventSource(url: string, callbacks: {
  onProgress?(progress: SyncProgress): void; onComplete?(event: SyncJobEvent): void; onError?(): void;
}): EventSource;
declare function escapeHtml(value: unknown): string;
declare function escapeAttr(value: unknown): string;
declare function openLightbox(id: number, filename: string, photos: {id: number; filename: string}[]): void;
declare function closeLightbox(): void;
interface Window {vireoThumbnailUrl?(photo: SyncPhoto): string; _lightboxPhotoList?: {id: number; filename: string}[]}
