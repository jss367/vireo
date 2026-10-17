// Universal filter expression and page integration contracts.
type FilterScalar = string | number | boolean;
type FilterValue = FilterScalar | FilterScalar[] | {n: number; unit: string} | null;
interface FilterNodeFields {field?: string; op?: string; value?: FilterValue; value_label?: string; label?: string; mode?: string; rules?: FilterRule[]; _qs?: boolean; _qs_scope?: string; _qs_text?: string; _qs_version?: number; case?: boolean}
type FilterRule = FilterGroup | FilterLeaf;
interface FilterGroup extends FilterNodeFields {mode: string; rules: FilterRule[]}
interface FilterLeaf extends FilterNodeFields {field: string; op: string; value: FilterValue}
interface FilterField {key?: string; label: string; type: string; default_op?: string; ops: string[]; values?: FilterScalar[]; labels?: Record<string, string>; changed_by?: string[]; hidden?: boolean; suggest?: boolean; picker?: string; pages?: string[]; category?: string; picker_hidden?: boolean; case_toggle?: boolean}
interface FilterVisual {prompt: string; strength: string}
interface FilterVisualInfo {status?: string; message?: string}
interface FilterScope {folder_id?: number | null; collection_id?: number | null}
interface FilterMutationOptions {noSnapshot?: boolean; lightRender?: boolean; noPersist?: boolean; silent?: boolean; reason?: string}
interface FilterShortcut {id: string; kind: string; field: string; value?: string; group?: string; label: string; rules: FilterRule}
interface FilterShortcutGroup {style: string; shortcuts: FilterShortcut[]}
interface FilterSnapshot {root: FilterGroup; muted: boolean; visual: FilterVisual | null}
interface FilterState extends FilterSnapshot {page: string; visualInfo: FilterVisualInfo | null; scopeLabel: string; scopeLocked: boolean; fields: Record<string, FilterField> | null; fieldOrder: string[]; excludedValues: Record<string, FilterScalar[]>; onCollectionSaved: (() => void) | null; onChange: ((change: {reason: string | null}) => void) | null; getContextRules: (() => FilterRule[]) | null; getScope: (() => FilterScope) | null; workspaceId: number | null; resultTotal: number | null; wouldMatch: number | null; advanced: boolean; ready: boolean; shortcuts: FilterShortcut[]}
interface FilterInitOptions {page?: string; root?: HTMLElement | string | null; scopeLabel?: string; scopeLocked?: boolean; excludedValues?: Record<string, FilterScalar[]>; onChange?: FilterState['onChange']; getContextRules?: FilterState['getContextRules']; getScope?: FilterState['getScope']; onCollectionSaved?: () => void; skipRestore?: boolean; handoffPages?: string[]}
interface FilterChip {visual?: boolean; error?: boolean; label: string; node?: FilterRule; qs?: boolean}
interface FilterApi {init(options: FilterInitOptions): Promise<boolean>; getRules(): FilterRule | FilterRule[]; opLabels(): Record<string, string>; describeRule(rule: FilterRule, fields?: Record<string, FilterField>): string; getVisual(): FilterVisual | null; setVisualInfo(info: FilterVisualInfo | null): void; visualSearch(text: string): void; loadExpression(rules: FilterRule | FilterRule[], visual?: FilterVisual | null, options?: FilterMutationOptions): void; getUserRules(): FilterGroup; addRule(field: string, op?: string, value?: FilterValue): void; selectPhotoGroup(photoIds: number[], label: string): void; quickSearch(text: string): void; removeField(field: string, options?: FilterMutationOptions): boolean; hasFilters(): boolean; dependsOnMutation(mutations: string | string[]): boolean; clearAll(silent?: boolean): void; isReady(): boolean; isMuted(): boolean; setScopeLabel(label: string): void; setResultTotal(total: number): void; refresh(): void}
interface Window {VireoFilter: FilterApi}
declare function escapeHtml(value: unknown): string;
declare function preferredScrollBehavior(): ScrollBehavior;

interface FilterFacet {value: string; count: number}
interface FilterFolder {id: number; parent_id: number | null; path: string; name?: string}
type FilterPickerEntry = FilterFacet | FilterFolder;
interface FilterPicker {field: string; query: string; open: boolean; values: (FilterFacet | FilterFolder)[] | null; scopedPaths: Set<string> | null; scopeKey: string | null; error: boolean; request: number; pending: boolean; timer?: ReturnType<typeof setTimeout>; collapsed: Set<number>}
interface FilterWorkspace {id: number; ui_state?: unknown}
interface FilterUiState {universal_filters?: Record<string, FilterSnapshot>; [setting: string]: unknown}
interface FilterVisualInfo {candidates?: number; coverage?: number; indexed?: number; total?: number; available?: number; pending?: number; error?: string}

interface Window {VireoSearch: {parse(text: string, options: {field: string}): FilterRule}}

interface FilterRegisteredField extends FilterField {key: string}
