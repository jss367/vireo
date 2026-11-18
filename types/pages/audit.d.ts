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
interface AuditClockFix {
  dependent_fixes?: AuditClockFix[];
  expected_state: string; group: string; count: number; first: string; last: string; refs: number[];
  recorded_offset: string | null; target_offset: string | null; shift_minutes: number;
  label: string; explanation: string; recommended: boolean;
}
interface AuditClockFinding {
  folder_id: number; folder_path: string; camera: string | null; summary: string;
  evidence: string[]; fixes: AuditClockFix[]; photo_count: number; jump_count: number;
}
interface AuditClockResult {findings: AuditClockFinding[]; location_available: boolean}
interface AuditCaptureTimePreview {
  count: number;
  samples: {filename: string; before_time: string | null; before_offset: string | null; after_time: string | null; after_offset: string | null}[];
}
interface AuditCaptureTimeJob {
  status: string; error?: string | null;
  progress?: {current?: number; total?: number} | null;
  result?: {updated: number; skipped: number; failed: number} | null;
}
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
  timeAgo(iso: string | null | undefined): string;
  safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  showToast?(message: string, kind?: string): void;
}
interface AuditSubjectPhoto {
  id: number; filename: string; detection_id: number | null; species?: string | null;
  bucket_size?: number; weak?: boolean; whole_frame?: boolean;
  unnamed?: number; stale_mask?: number; stale_score?: number;
}
interface AuditSubjectReport {
  findings: AuditSubjectPhoto[]; review: AuditSubjectPhoto[]; references: AuditSubjectPhoto[];
}
