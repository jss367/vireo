// Settings page values, schema controls, and separately checked shared APIs.
type SettingsValue = null | boolean | number | string | SettingsValue[] | {[key:string]:SettingsValue};
interface SettingsSpec {type:string;scope?:string;label:string;desc?:string;category?:string;enum?:string[];enum_labels?:Record<string,string>;nullable?:boolean;null_label?:string;step?:number;min?:number;max?:number;items_enum?:string[]}
interface SettingsValues {workspace:Record<string,SettingsValue>;global:Record<string,SettingsValue>;default:Record<string,SettingsValue>}
interface SettingsRule {field?:string;op?:string;value?:SettingsValue;mode?:string;rules?:SettingsRule[]}
interface SettingsShortcut {id:string;label:string;group:string;rules:SettingsRule}
interface SettingsFieldSpec {key:string;label?:string;type:string;ops:string[];values?:(string|number)[];labels?:Record<string,string>;category?:string;enum?:string[];pages?:string[];min?:number;max?:number;step?:number}
interface SettingsEditor {name:string;path:string}
interface SettingsRemoteTarget {id:string;name:string;host:string;user:string;port:number|string;ssh_key:string;remote_path:string;mount_path:string;local_archive_root:string;bwlimit_kbps:number|string}
interface SettingsModel {id:string;name:string;description?:string;role?:string;status?:string;state?:string;size?:string;size_estimate?:string;downloaded?:boolean;missing_optional_files?:string[];architecture?:string;model_str?:string;parameters?:string;size_mb?:number;verify_skipped_reason?:string;weights_path?:string;source?:string}
interface SettingsMount {mount_point:string;host:string;user?:string;display_name?:string;remote_path?:string;fstype?:string;share?:string}
interface SettingsNasWizard {step:string;mounts:SettingsMount[];mount:SettingsMount|null;host:string;user:string;port:number;keyAuthOk:boolean;pubKeyLine:string;remotePath:string;archiveRoot:string;browsePath:string;remoteBrowsePath:string;keyPath?:string;sshKey?:string;sshInfo?:unknown;name?:string}
interface SettingsJobProgress {phase?:string;percent?:number;current?:number;total?:number;current_file?:string;message?:string}
interface SettingsJobComplete {status:string;result?:SettingsJobResult;failure?:{message?:string};errors?:string[]}
interface SettingsQuotaError {requested_working_copy_quota_mb?:number;current_working_copy_usage_bytes?:number}
declare function safeFetch<T>(url:string,options?:RequestInit,feedback?:{toast?:boolean}):Promise<T>;
declare function safeEventSource(url:string,callbacks:{onProgress?:(progress:SettingsJobProgress)=>void;onComplete?:(result:SettingsJobComplete)=>void;onError?:()=>void}):unknown;
declare function showToast(message:string,kind?:string):void;
declare function escapeHtml(value:unknown):string;
declare function escapeAttr(value:unknown):string;
declare function formatBytes(value:number):string;
declare function isTauri():boolean;
declare function pickFile(options:{title:string}):Promise<string|null>;
declare function pickDirectory(title:string,options?:{defaultPath?:string}):Promise<string|null>;
declare function checkForAppUpdate():Promise<{available:boolean;version?:string;notes?:string}|null>;
declare function downloadAndInstallUpdate():Promise<boolean>;
declare function relaunchApp():void;
declare function checkPendingSync():void;
declare function appendRsyncInstallCommands(container:HTMLElement|null,commands:unknown):void;
declare function preferredScrollBehavior():ScrollBehavior;
declare const VireoFilter:{describeRule(rule:SettingsRule,fields:Record<string,SettingsFieldSpec>):string;opLabels():Record<string,string>};
declare const VireoTextSearch:{readOptions(prefix:string):{caseSensitive?:boolean;wholeWord?:boolean;regex?:boolean};matchesFields(fields:string|string[],query:string,options:ReturnType<typeof VireoTextSearch.readOptions>):boolean};
declare const VireoPipelineConfig:{pipelineFromConfig(config:SettingsConfig):Record<string,number|boolean>;percent(value:unknown,key:string):number;asNumber(value:unknown,key:string):number;embeddingThresholdToDistancePercent(value:unknown):number;embeddingDistancePercentToThreshold(value:number):number;buildSliderDefaults():{grouping:{burst_embedding_distance:number}};defaultPipeline():Record<string,number|boolean>};
declare class VireoFolderBrowser {constructor(options:{overlayId:string;modes:Record<string,{title:string|(()=>string);startPath:()=>string;onSelect:(path:string)=>void}>});sequence:number;open(mode:string,options?:{skipInitialBrowse:boolean}):void}
interface Window {VireoFilter?:typeof VireoFilter;VIREO_EDITOR_PATH_PLACEHOLDER:string;_dtAsset?:SettingsDarktableAsset;getThemeList?():{id:string;name:string;description?:string;color?:string;icon?:string}[];invalidateEditorsCache?():void;vireoSetNumberedNavigationMode(mode:string):void}

interface SettingsConfig {
  move_keep_visible_in_other_workspaces?: boolean;
  classification_threshold?: number;
  match_thresholds?: Record<string,SettingsValue>;
  grouping_window_seconds?: number;
  similarity_threshold?: number;
  preview_max_size?: number;
  keyword_case?: string;
  sync_flags_to_xmp?: boolean;
  write_assigned_location_to_xmp?: boolean;
  write_location_keywords_to_xmp?: boolean;
  max_edit_history?: number;
  grouping_history_max_mb?: number;
  retained_cache_max_mb?: number;
  retained_cache_max_age_days?: number;
  inat_token?: string;
  hf_token?: string;
  google_maps_api_key?: string;
  google_maps_prefer_english?: boolean;
  scan_roots?: string[];
  scan_workers?: number;
  setup_complete?: boolean;
  rsync_bin?: string;
  ssh_bin?: string;
  remote_targets?: SettingsRemoteTarget[];
  darktable_bin?: string;
  external_editor?: string;
  external_editors?: SettingsEditor[];
  export_presets?: SettingsValue[];
  report_url?: string;
  darktable_style?: string;
  darktable_output_format?: string;
  darktable_output_dir?: string;
  darktable_auto_convert_dng?: boolean;
  dng_converter_bin?: string;
  open_in_browser?: boolean;
  subject_types?: SettingsValue[];
  browse_card_fields?: string[];
  filter_shortcuts?: SettingsShortcut[];
  photos_per_page?: number;
  thumbnail_size?: number;
  thumbnail_quality?: number;
  working_copy_max_size?: number;
  working_copy_quality?: number;
  working_copy_cache_max_mb?: number;
  preview_quality?: number;
  preview_cache_max_mb?: number;
  browse_thumb_default?: number;
  browse_stack_time_gap?: number;
  browse_stack_split_mode?: string;
  detector_confidence?: number;
  detection_padding?: number;
  top_k_predictions?: number;
  redundancy_threshold?: number;
  cull_time_window?: number;
  cull_phash_threshold?: number;
  pipeline?: Record<string,SettingsValue>;
  ingest?: Record<string,SettingsValue>;
  numbered_navigation_mode?: string;
  keyboard_shortcuts?: Record<string,SettingsValue>;
}
interface SettingsLabel {labels_file:string;name:string;taxon_groups?:string[];usable_count?:number;species_count?:number;ambiguous_count?:number}
interface SettingsEmbeddingPrecompute {job_id?:string;queued?:boolean;status?:string;model_id?:string;labels_file?:string;model_name?:string}
interface SettingsJobResult {error?:string;removed?:number;downloaded?:number;species_count?:number;embedding_precompute?:SettingsEmbeddingPrecompute;ok?:string[];failed?:string[];verified?:boolean;config_written?:boolean;bin_path?:string;downloaded_to?:string;quarantined?:boolean}
interface SettingsPlatformReadiness {platform?:string;support_tier?:string;windows_release?:string;architecture?:string;webview2_version?:string;long_paths?:{enabled:boolean};dependencies?:Record<string,{state:string;required?:boolean;path?:string;hint?:string;install_commands?:string[]}>}
interface SettingsDarktableAsset {platform?:string;version?:string;name?:string;size?:number;digest?:string;url?:string}

interface ModelRemovalResponse {ok?:boolean;kept_path?:string|null;cache_cleanup?:{freed:number;failed:Array<{error:string}>};}
