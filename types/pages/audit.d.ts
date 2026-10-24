// The Audit page (vireo/static/audit/): the /api/audit/* payloads it renders
// and the shared globals its shared.js reads.
interface AuditDrift {
  photo_id: number; filename: string; field: string; db_value: unknown; xmp_value: unknown;
  direction?: string; resolutions?: string[];
  xmp_problem?: 'corrupt' | 'unreadable'; xmp_error?: string; advice?: string;
}
interface AuditOrphan {photo_id: number; filename: string; folder_path: string}
interface AuditFileEntry {path: string; folder: string}
interface AuditFlaggedHash {photo_id: number; filename: string; folder_path: string; hash_status: string; hash_checked_at: string}
interface AuditIntegrity {stats: {checked: number; total: number; unchecked: number}; flagged: AuditFlaggedHash[]}
interface AuditCheckRun {problem_count: number; ran_at: string}
interface AuditSummary {
  status: string; problem_count: number; missing_folders: number; missing_folder_photos: number;
  integrity: {checked: number; total: number; unchecked: number};
  checks: Record<string, AuditCheckRun | null>;
}
interface AuditVerifyJob {
  status: string;
  progress?: {current?: number; total?: number; current_file?: string} | null;
  result?: {checked: number; baselined?: number; missing?: number; modified?: number; corrupt?: number; unreadable?: number} | null;
}
interface Window {
  escapeHtml(value: unknown): string;
  escapeAttr(value: unknown): string;
  safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  showToast?(message: string, kind?: string): void;
}
