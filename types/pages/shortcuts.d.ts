// The Keyboard Shortcuts page (vireo/static/shortcuts/): the bindings it
// edits and the shared globals its shared.js reads.
/** Bindings per context (`navigation`, `browse`, ...), action name to key string. */
type ShortcutConfig = Record<string, Record<string, string | undefined>>;
interface ShortcutParts {key: string; ctrl: boolean; meta: boolean; shift: boolean; alt: boolean}
interface Window {
  safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  parseShortcut(shortcut: string): ShortcutParts;
  formatShortcut(shortcut: string | undefined): string;
  Keymap?: {
    pauseDispatch?(handler: (event: KeyboardEvent) => void): void;
    resumeDispatch?(): void;
  };
}
