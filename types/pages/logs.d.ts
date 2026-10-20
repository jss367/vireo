/// <reference path="../time.d.ts" />
// The Logs page (vireo/static/logs/): the log records it renders and the
// shared globals its shared.js reads.
interface LogRecord {time: number; level: string; logger?: string; message?: string}
interface LogSearchOptions {matchCase?: boolean; wholeWord?: boolean}
interface Window {
  escapeHtml(value: unknown): string;
  VireoTextSearch: {
    readOptions(namespace: string): LogSearchOptions;
    matchesFields(fields: string, query: string, options: LogSearchOptions): boolean;
    toggle(namespace: string, option: string, callback: () => void): void;
  };
  safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  showToast?(message: string, kind?: string): void;
}
