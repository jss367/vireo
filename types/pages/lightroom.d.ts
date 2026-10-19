// The Lightroom Import page (vireo/static/lightroom/): the preview and job
// payloads it renders and the shared globals its shared.js reads.
interface LightroomCatalogPreview {
  catalog: string; total_files: number; matched_files: number; unmatched_files: number; keyword_count: number;
}
interface LightroomConflict {file_path: string; keywords_by_catalog: Record<string, string[]>}
interface LightroomPreview {
  catalogs?: LightroomCatalogPreview[]; conflict_count: number; conflicts?: LightroomConflict[];
}
interface LightroomJobComplete {
  status: string; duration?: number;
  result?: {imported?: number; skipped?: number; failed?: number};
  errors?: string[];
}
interface LightroomJobCallbacks {
  onProgress?(progress: {current: number; total: number}): void;
  onComplete?(result: LightroomJobComplete): void;
  onError?(): void;
}
interface Window {
  escapeHtml(value: unknown): string;
  safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  safeEventSource(url: string, callbacks: LightroomJobCallbacks): EventSource;
  isTauri?(): boolean;
  pickFile(options: {title: string; filters: {name: string; extensions: string[]}[]}): Promise<string | null>;
}
