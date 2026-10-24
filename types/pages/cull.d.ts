type CullAction = 'keep' | 'review' | 'reject';
interface CullSourcePhoto {
  id: number; filename: string; timestamp?: string | null;
  confirmed_species?: string | string[]; species_top5?: [string, number][];
  flag_confirmation_status?: 'confirmed' | 'unverified';
  flag?: string; rating?: number; width?: number; height?: number;
  quality_composite?: number | null; subject_sharpness?: number | null; label?: string;
}
type CullBurst = number[] | {photo_ids: number[]};
interface CullEncounter {
  photo_ids: number[]; photo_count?: number; bursts?: CullBurst[]; burst_count?: number;
  confirmed_species?: string | string[]; species?: string | string[];
  species_predictions?: {species: string | string[]}[];
  time_range?: [string | null, string | null];
}
interface CullSourceResults {photos: CullSourcePhoto[]; encounters: CullEncounter[]; summary?: Record<string, unknown>;}
interface CullDisplayPhoto {
  photo_id: number; filename: string; quality: number; day: string; burst_id: string;
  action: CullAction; decision_source: string; suggested_action: CullAction;
  favorite?: boolean; group_index?: number; quality_score?: number | null; sharpness?: number | null;
  timestamp?: string | null; pipeline_label?: string;
}
interface CullSceneGroup {photos: CullDisplayPhoto[]; scene_id?: number; label?: string;}
interface CullSpeciesGroup {
  species: string; scene_groups: CullSceneGroup[]; pose_groups?: CullSceneGroup[];
  keepers: number; reviews: number; rejects: number; photo_count: number;
}
interface CullData {
  species_groups: CullSpeciesGroup[]; total_photos: number; suggested_keepers?: number;
  suggested_reviews?: number; suggested_rejects?: number; pinned_count?: number; confirmed_count?: number;
  saved_flag_count?: number; pinned_offscreen?: number; pipeline_summary?: Record<string, unknown>;
}
/** A scene group by its species and group index (null when nothing is focused). */
interface CullGroupRef {speciesIdx: number | null; groupIdx: number | null;}
interface CullDecision {action: CullAction; source: string; suggested: CullAction;}
interface CullCompareController {
  open(ids: number[]): Promise<void> | undefined;
  close(event?: Event): void; isOpen(): boolean; resetViews(): void;
}
interface CullCompare {
  controller: CullCompareController; close(event?: Event): void; isOpen(): boolean;
  resetViews(): void; step(delta: number): void;
}
declare function openLightbox(id: number, filename: string, photos: {id: number; filename: string}[]): void;
declare function showDeleteDialog(ids: number[], count: number, onComplete: (data: {failed_photo_ids?: number[]}) => void): void;
declare function safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
declare function showToast(message: string, type?: string): void;
declare var Vireo: VireoPageHookRegistry;
declare var VireoPipelineConfig: {embeddingDistancePercentToThreshold(value: number): number; buildSliderDefaults(config?: unknown): {scoring: Record<string, number>; grouping: Record<string, number>}};
declare function escapeHtml(value: unknown): string;
declare function escapeAttr(value: unknown): string;
declare const VireoBrowseCompare: {create(options: {
  findPhoto(id: number): CullSourcePhoto | undefined;
  fetch: typeof safeFetch; showToast: typeof showToast; syncZoom: boolean;
}): CullCompareController;
  bindControls(document: Document, getCompare: () => Pick<CullCompare, 'close' | 'resetViews' | 'step'> | null): void;
};
interface Window {vireoThumbnailUrl?(photo: CullDisplayPhoto): string; vireoHistoryBusy?(): boolean;}
interface DocumentEventMap {
  'lightbox:flagchanged': CustomEvent<{photoId: number; flag: string}>;
  'lightbox:photodeleted': CustomEvent<{photoId: number}>;
}
