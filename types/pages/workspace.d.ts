// The Workspace page (vireo/static/workspace/): the workspace and job-history
// payloads it renders and the shared globals its shared.js reads. The Work
// Locally types and `window.vireoLocalFolders` come from work-locally.d.ts.
interface WorkspaceFolder {id: number; path: string; workspace_photo_count?: number}
interface ActiveWorkspace {id: number; name: string; folders?: WorkspaceFolder[]}
interface WorkspaceListEntry {id: number; name: string; folder_count?: number}
interface JobHistoryRow {
  type: string; status: string; started_at?: string | null; duration?: number | null;
  summary?: string | null; result?: unknown; error_count?: number;
}
interface MoveFoldersResult {folders_moved: number; detections_moved: number; pending_changes_moved: number}
interface Window {
  escapeHtml(value: unknown): string;
  escapeAttr(value: unknown): string;
  safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  showToast?(message: string, kind?: string): void;
  formatDuration(seconds: number): string;
  vireoWorkspaceSwitcher: {showCreate(): void};
  vireoEditNav?: {clearWorkspaceScopedCursors(): void};
}
