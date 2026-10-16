// Globals every page gets from vireo/static/navbar-shared-helpers.js, which the
// shared navbar loads before any page script. Declared here for the strict
// `checkJs` files that call them; the implementations stay classic scripts.

interface SafeEventSourceCallbacks {
  onProgress?: (data: any) => void;
  onComplete?: (data: any) => void;
  onError?: () => void;
  // Suppress the generic "Connection lost" toast because onError shows a
  // more specific one.
  quietError?: boolean;
}

declare function showToast(msg: string, type?: string): void;
declare function safeEventSource(url: string, callbacks?: SafeEventSourceCallbacks): EventSource;
