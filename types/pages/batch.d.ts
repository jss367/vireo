interface BatchEditField {
  path: string; label: string; group: string;
  min?: number; max?: number; step?: number; default?: number;
}
interface NumericBatchEditField extends BatchEditField {min: number; max: number; step: number; default: number;}
interface BatchEditSelectionOptions {title?: string; hint?: string; fields?: string[]; available?: string[]; action?: string;}
type BatchRecipe = Record<string, unknown>;
interface BatchEditResult {
  count: number; skipped?: number[]; local_errors?: Record<string, string>;
  recipes?: Record<string, BatchRecipe>;
}
interface BatchEditPreset {id: number; name: string; fields?: string[]; recipe?: BatchRecipe;}
interface BatchEditController {
  fields(): Promise<BatchEditField[]>;
  selectFields(options: BatchEditSelectionOptions): Promise<string[] | null>;
  apply(ids: number[], recipe: BatchRecipe, fields: string[], mode?: string, description?: string): Promise<BatchEditResult>;
  paste(ids: number[]): Promise<BatchEditResult | null>;
  open(ids: number[]): Promise<void>;
  resultMessage(result: BatchEditResult): string;
}
interface Window {
  VireoBatchEdits: BatchEditController;
  vireoRefreshEditRecipeCache?(recipes: Record<string, BatchRecipe>): void;
  vireoEditNav?: {getCopiedRecipe(): {recipe: BatchRecipe} | null};
}
declare function safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
