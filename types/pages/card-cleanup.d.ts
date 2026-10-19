// The card cleanup page (vireo/static/card-cleanup/): the scan manifest and
// job payloads it renders, and the shared globals its shared.js reads.
interface CardCleanupEntry {path: string; bucket: string; size?: number; reason?: string; archive_path?: string; hash?: string}
interface CardCleanupTotal {count: number; bytes: number}
interface CardCleanupManifest {
  source_root?: string; entries?: CardCleanupEntry[]; totals?: Record<string, CardCleanupTotal | undefined>;
  walk_errors?: string[]; incomplete?: boolean; [key: string]: unknown;
}
interface CardCleanupJob {
  id?: string; type?: string; status: string; error?: string | null; errors?: string[];
  result?: {modified?: number; corrupt?: number; unreadable?: number; archive_files_read?: number; unblocked_files?: number; [key: string]: unknown} | null;
  progress?: {current?: number; total?: number; phase?: string; current_file?: string} | null;
}
interface CardCleanupDeleteResult {
  deleted?: number; deleted_bytes?: number; skipped?: {path: string; reason?: string}[]; failed?: {path: string; error?: string}[];
  skipped_count?: number; failed_count?: number; [key: string]: unknown;
}
interface Window {
  formatBytes(bytes: number | null | undefined): string;
  VireoFolderBrowser: new (options: FolderBrowserOptions) => {
    readonly sequence: number;
    open(mode: string, options?: {skipInitialBrowse?: boolean}): void;
    close(): void; browse(path: string | null): Promise<void>; confirm(): void;
  };
  pickDirectory(title: string): Promise<string | string[] | null>;
  isTauri?(): boolean;
}
interface FolderBrowserOptions {
  overlayId: string; defaultMode?: string; onError?(message: string): void;
  modes: Record<string, {title?: string; multiple?: boolean; showCounts?: boolean; fileTypes?: string;
    startPath?(): string; onSelect?(path: string): void}>;
}
