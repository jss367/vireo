// Shared browser dependencies, loaded by the navbar before Move's classic scripts.
interface MoveFolder { id: number; path: string; name?: string; photo_count?: number; }
interface MovePhoto { id: number; filename: string; thumbnail_path?: string | null; }
interface MoveCriteria { rating_min?: number; flag?: string; species?: string[]; has_predictions?: boolean; folder_ids?: number[]; imported_before?: string; }
interface MoveRule { id: number; name: string; destination: string; criteria?: MoveCriteria | string; last_run_at?: string | null; }
interface MoveRemoteTarget { id: string; name: string; user: string; host: string; remote_path: string; mount_path?: string; }
interface MoveDestination { folder_id?: number; destination?: string; remote_target_id?: string; subpath?: string; destination_name?: string; folder_template?: string; keep_visible?: boolean; merge?: boolean; }
interface MoveDateSamples { samples: Record<string, string>; dated_count: number; }
interface MovePreflight { date_organized?: boolean; destinations?: { path: string; photo_count: number; exists: boolean }[]; photo_count: number; destination_count: number; template_samples?: MoveDateSamples; remote?: boolean; reachable?: boolean; error?: string; exists?: boolean; file_count: number; file_count_truncated?: boolean; resolved_dest: string; preview?: {will_copy?: number; will_skip?: number; will_block?: number}; }
interface MoveFolderDetails { sourcePath: string; destinationPath: string; photoCount: number | null; remote: boolean; }
interface MoveMergeDetails { sourcePath: string; destinationPath: string; photoCount: string | number; copyCount: number | null; skipCount: number | null; blockCount: number; destinationFileCount: number; destinationFileCountTruncated: boolean; remote: boolean; }
interface MoveBrowserData { path: string; dirs?: {path: string; name: string}[]; }
interface MoveConfig { move_keep_visible_in_other_workspaces?: boolean; ingest?: { recent_destinations?: unknown }; }
interface MoveVisibility {workspaces?: {name: string; photo_count: number}[];}
// safeFetch as this page calls it: one signature per endpoint it reads.
interface MoveSafeFetch {
  (url: '/api/folders', init?: RequestInit, options?: {toast?: boolean}): Promise<MoveFolder[]>;
  (url: '/api/move-rules', init?: RequestInit): Promise<MoveRule[]>;
  (url: '/api/remote-targets', init?: RequestInit): Promise<{targets?: MoveRemoteTarget[]; rsync_available?: boolean}>;
  (url: '/api/config', init?: RequestInit, options?: {toast?: boolean}): Promise<MoveConfig>;
  (url: '/api/recent-destinations', init?: RequestInit, options?: {toast?: boolean}): Promise<{recent_destinations?: unknown}>;
  (url: '/api/move-photos/visibility', init?: RequestInit): Promise<MoveVisibility>;
  (url: '/api/move-folder/preflight', init?: RequestInit): Promise<MovePreflight>;
  (url: '/api/move-rules/preview', init?: RequestInit): Promise<{photo_ids?: number[]}>;
  (url: '/api/jobs/move-folder', init?: RequestInit): Promise<{job_id: number}>;
  (url: '/api/jobs/move-photos', init?: RequestInit): Promise<{job_id: number}>;
  (url: '/api/browse', init?: RequestInit): Promise<MoveBrowserData>;
  (url: '/api/volumes', init?: RequestInit): Promise<NonNullable<MoveBrowserData['dirs']>>;
  <T = unknown>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
}
// The shared globals the page's shared.js reads.
interface Window {
  vireoThumbnailUrl?: (photo: MovePhoto) => string;
  escapeHtml(value: unknown): string;
  escapeAttr(value: unknown): string;
  showToast?(message: string, kind?: string): void;
  rsyncInstallHint(): string;
  safeFetch: MoveSafeFetch;
}
