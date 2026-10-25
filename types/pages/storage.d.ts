interface StorageOverviewCategory {name:string;size:number;type?:string|null;release?:boolean;settings?:boolean;hfCache?:boolean;note?:string|null;count?:number;kind:string;kindLabel:string}
interface HfCacheEntry {retained_reason?:string|null;removable?:boolean;retained_size?:number;name:string;key:string;size:number;file_count:number;installed:boolean;install_path?:string}
interface HfCacheInventory {retained_size?:number;entries:HfCacheEntry[];unused_size:number;installed_size?:number;failed?:Array<{path:string;error:string}>;freed?:number}
interface RetainedStoragePolicy {retained_cache_max_mb:number;retained_cache_max_age_days:number;grouping_history_max_mb:number}
interface RetainedStorageResponse {maintenance?:{status:string;finished_at?:string;started_at:string;summary?:string};caches:Array<{name:string;size:number;count:number;errors:string[];path:string;type:string;clearable:boolean;files:Array<{name:string;size:number}>;truncated?:boolean}>;grouping_history:{bytes:number;count:number;redo_bytes:number;entries:Array<{workspace:string;description:string;bytes:number;undone:boolean}>;truncated?:boolean};policy:RetainedStoragePolicy}

// /api/storage
interface StorageVolume {name?:string;free?:number|null;capacity?:number|null}
interface StorageLocation {id:string;path?:string;labels?:string[];volume?:StorageVolume}
interface StorageCount {size:number;count:number}
interface StorageWorkingCopies {size:number;count:number;quota_bytes:number}
interface StorageAuxiliaryCategory {name:string;size:number;entries:Array<{size:number;description:string;path:string}>}
interface StorageMaskVariant {variant:string;count?:number;bytes?:number;active_count?:number}
interface StorageMasks {variants?:StorageMaskVariant[];total_bytes?:number;stale_count?:number}
interface StorageReport {
  total:number;reclaimable:number;volumes?:StorageVolume[];locations?:StorageLocation[];
  working_copies:StorageWorkingCopies;database:{size:number};thumbnails:StorageCount;previews:StorageCount;
  embeddings:StorageCount;models:{size:number};hf_cache:HfCacheInventory&{size:number};offline_originals:StorageCount;
  other:{categories?:StorageAuxiliaryCategory[]};masks?:StorageMasks;
}
interface StorageFile {name:string;size:number;meta?:{model?:string;label_count?:number;created?:string}|null}
interface StorageConfigValues {thumbnail_size?:number|null;thumbnail_quality?:number|null;preview_quality?:number|null;preview_cache_max_mb?:number|null;working_copy_cache_max_mb?:number|null}
/** safeFetch's rejection: the server's status, error code and body ride on the Error. */
interface StorageRequestError extends Error {status?:number;code?:string;body?:{previous_working_copy_quota_mb?:number;current_working_copy_usage_bytes?:number}}
interface StorageEmbeddingMatrix {
  models?:Array<{id:string;name:string}>;
  matrix?:Array<{labels_name:string;labels_file:string;species_count:number;unusable?:boolean;skipped?:number;models:Record<string,{cached?:boolean}|undefined>}>;
}

// The shared globals the page's shared.js reads.
interface Window {
  VireoReleaseOriginals:{storageButton():string};
  Vireo:VireoPageHookRegistry;
  safeFetch<T = unknown>(url:string,options?:RequestInit,settings?:{toast?:boolean}):Promise<T>;
  safeEventSource(url:string,callbacks:{onComplete?():void}):EventSource;
  showToast?(message:string,kind?:string):void;
}
