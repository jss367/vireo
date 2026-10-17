// Page inline scripts supply these callbacks; shared implementations are
// checked in this program instead of being replaced with ambient stubs.
declare function loadWorkspaces(): Promise<void>;
declare function isTauri(): boolean;
declare const L: {map(containerId: string, options: Record<string, unknown>): {
  invalidateSize(options: {pan: boolean; debounceMoveend: boolean}): void;
}};
declare function buildExportPreflightRequest(): ExportPreflightRequest | null;
declare function loadStoragePanel(): Promise<void>;
