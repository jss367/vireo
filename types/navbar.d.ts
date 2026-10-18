// Globals every page gets from the shared navbar: navbar-globals.js publishes
// these exports of navbar-shared-helpers.js before any page script runs.
// Declared here for the strict `checkJs` files that call them.

interface SafeEventSourceCallbacks<Progress = unknown, Complete = unknown> {
  onProgress?: (data: Progress) => void;
  onComplete?: (data: Complete) => void;
  onError?: () => void;
  // Suppress the generic "Connection lost" toast because onError shows a
  // more specific one.
  quietError?: boolean;
}

declare function showToast(msg: string, type?: string): void;
declare function safeEventSource<Progress = unknown, Complete = unknown>(url: string, callbacks?: SafeEventSourceCallbacks<Progress, Complete>): EventSource;
