interface SavedExportSettings {
  destination?: string; export_to_subfolder?: boolean; subfolder_name?: string;
  reveal_after_export?: boolean; format?: string; max_size?: number | null;
  quality?: number; naming_template?: string; metadata_fields?: string[];
}
interface SavedExportPreset {name: string; settings: SavedExportSettings;}
interface SaveExportPresetResult {conflict?: boolean; error?: string; keptCurrent?: boolean;}
interface ExportBuiltinOption {value: string; label: string | null;}
type ExportPresetControl = HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement | HTMLButtonElement;
interface ExportPresetControls {
  exportOverlay: HTMLElement; exportPreset: HTMLSelectElement;
  exportSubfolderName: HTMLInputElement; exportSubfolder: HTMLInputElement;
  exportDest: HTMLInputElement; exportRevealAfter: HTMLInputElement;
  exportFormat: HTMLSelectElement; exportResize: HTMLSelectElement;
  exportResizeCustom: HTMLInputElement; exportQuality: HTMLInputElement;
  exportQualityVal: HTMLElement; exportTemplate: HTMLInputElement;
  exportSubmitBtn: HTMLButtonElement; exportPresetSaveBtn: HTMLButtonElement;
  exportPresetDeleteBtn: HTMLButtonElement; exportPresetDialog: HTMLElement;
  exportPresetDialogError: HTMLElement; exportPresetDialogName: HTMLInputElement;
  exportPresetDialogCancelBtn: HTMLButtonElement; exportPresetDialogSubmitBtn: HTMLButtonElement;
  exportPresetDialogDescription: HTMLElement; exportPresetDialogTitle: HTMLElement;
  exportPresetDialogNameField: HTMLElement; exportPresetDialogForm: HTMLFormElement;
}
declare function safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
declare function showToast(message: string, type?: string): void;
declare function updateExportFormatControls(): void;
declare function updateExportPreview(): void;
declare function selectedExportMetadataFields(): string[];
declare function applyExportPreset(value: string): void;
declare function markExportCustom(): void;
declare const VireoViewPreferences: {
  read(key: string): string | null;
  write(key: string, value: string): void;
  persist(element: Element): void;
};
