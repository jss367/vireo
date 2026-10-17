type CullAction = 'keep' | 'review' | 'reject';
interface CullSourcePhoto {
  id: number; filename: string; timestamp?: string | null;
  confirmed_species?: string | string[]; species_top5?: [string, number][];
  flag?: string; rating?: number; width?: number; height?: number;
}
type CullBurst = number[] | {photo_ids: number[]};
interface CullEncounter {
  photo_ids: number[]; photo_count?: number; bursts?: CullBurst[]; burst_count?: number;
  confirmed_species?: string | string[]; species?: string | string[];
  species_predictions?: {species: string | string[]}[];
  time_range?: [string | null, string | null];
}
interface CullSourceResults {photos: CullSourcePhoto[]; encounters: CullEncounter[];}
interface CullDisplayPhoto {
  photo_id: number; filename: string; quality: number; day: string; burst_id: string;
  action: CullAction; decision_source: string; suggested_action: CullAction;
  favorite: boolean; group_index: number;
}
interface CullSceneGroup {photos: CullDisplayPhoto[];}
interface CullSpeciesGroup {
  species: string; scene_groups: CullSceneGroup[]; pose_groups?: CullSceneGroup[];
  keepers: number; reviews: number;
}
interface CullData {species_groups: CullSpeciesGroup[]; total_photos: number;}
interface CullCompareController {
  open(ids: number[]): Promise<void> | undefined;
  close(event?: Event): void; isOpen(): boolean; resetViews(): void;
}
interface CullCompare {
  controller: CullCompareController; close(event?: Event): void; isOpen(): boolean;
  resetViews(): void; step(delta: number): void;
}
declare var cullData: CullData | null;
declare var pipelineResults: CullSourceResults | null;
declare var cullAnalysisSeq: number;
declare var cullAnalysisPending: number;
declare var cullBusy: boolean;
declare var cullDirty: boolean;
declare var selectedCollectionId: number | null;
declare var _reflowTimer: number | null;
declare var _regroupTimer: number | null;
declare var expandedSpecies: Record<string, boolean>;
declare var cullManualDecisions: Record<number, CullAction>;
declare var cullAppliedDecisions: Record<number, CullAction>;
declare function speciesName(value: string | string[]): string;
declare function topSpeciesForPhoto(photo: CullSourcePhoto): string;
declare function speciesForEncounter(encounter: Partial<CullEncounter>, photos: Record<number, CullSourcePhoto>): string;
declare function rebuildCullDataFromPipeline(): void;
declare function runCulling(): Promise<void>;
declare function updateCullApplyButton(): void;
declare function cullOperationPending(): boolean;
declare function cullGroupsForSpecies(group: CullSpeciesGroup): CullSceneGroup[];
declare function renderCulling(): void;
declare function setCullAction(id: number, action: CullAction): void;
declare function unappliedPinCount(): number;
declare function getGroupingConfig(): Record<string, number>;
declare function getScoringConfig(): Record<string, number>;
declare function pipelineBody(config: Record<string, number>, sourceOnly: boolean): unknown;
declare function setPipelineResults(data: CullSourceResults, status: string, scoped: boolean): void;
declare function openLightbox(id: number, filename: string, photos: {id: number; filename: string}[]): void;
declare function showDeleteDialog(ids: number[], count: number, onComplete: (data: {failed_photo_ids?: number[]}) => void): void;
declare function safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
declare function showToast(message: string, type?: string): void;
declare function escapeHtml(value: unknown): string;
declare function escapeAttr(value: unknown): string;
declare const VireoBrowseCompare: {create(options: {
  findPhoto(id: number): CullSourcePhoto | undefined;
  fetch: typeof safeFetch; showToast: typeof showToast; syncZoom: boolean;
}): CullCompareController};
interface Window {vireoThumbnailUrl?(photo: CullDisplayPhoto): string; vireoHistoryBusy?(): boolean;}
interface DocumentEventMap {
  'lightbox:flagchanged': CustomEvent<{photoId: number; flag: string}>;
  'lightbox:photodeleted': CustomEvent<{photoId: number}>;
}
