// The Duplicates page (vireo/static/duplicates/): the /api/duplicates/*
// payloads it renders and the shared globals its shared.js reads.
interface DuplicatePhoto {id:number;filename?:string;path?:string;exists?:boolean;volume_offline?:boolean;workspaces?:string[];rating?:number;file_size?:number;rejected?:boolean;reason?:string}
interface DuplicateKeepResponse {file_hash:string;winner_id:number;loser_ids:number[];rating:number;flag:string}
interface DuplicateProposal {file_hash:string;winner:DuplicatePhoto;other_kept?:DuplicatePhoto[];losers:DuplicatePhoto[];status:string;empty_file_group?:boolean;all_missing?:boolean;all_offline?:boolean;_gi?:number}
interface DuplicateBucket {file_hashes:string[];folders:string[];group_count:number;total_size?:number;example_filenames?:string[]}
interface SameShotPhoto extends DuplicatePhoto {file_hash:string;timestamp?:string|null;phash?:string|null}
interface SameShotGroup {identity:[string,string,string,number];camera:string;serial_number:string;shutter_count:number;visual_evidence:'matching'|'different'|'unavailable';photos:SameShotPhoto[]}
interface DuplicateScan {proposals:DuplicateProposal[];buckets?:DuplicateBucket[];catalog_cleanup?:boolean;stale_group_count?:number;new_group_count?:number;changed_group_count?:number;group_count?:number;loser_count?:number;resolved_group_count?:number;resolved_loser_count?:number;same_shot_scanned?:boolean;same_shot_groups?:SameShotGroup[];same_shot_eligible_photos?:number}
interface DuplicateCategory {id:string;label:string;groups:number;extras:number;bytes:number}
interface DuplicateFailure {id:number;error?:string}
interface DuplicateSkip {id:number;reason:string}
interface DuplicateTrashResponse {trashed?:number;skipped?:DuplicateSkip[];failed?:DuplicateFailure[]}
interface DuplicateTrashSummary {trashed:number;skipped:DuplicateSkip[];failed:DuplicateFailure[];completed:number;total:number}
interface DuplicateTrashProgress {phase:string;total:number;completed:number;batchStart:number;batchEnd:number;trashed:number;skipped:number;failed:number}
interface DuplicateTrashState {owner?:object;pending:boolean;terminal:boolean;message:string}
interface DuplicateTrashError extends Error {trashSummary?:DuplicateTrashSummary}
interface DuplicateLastScan {found:boolean;result?:DuplicateScan;finished_at?:string}
interface DuplicateScanProgress {phase?:string;current_file?:string;current?:number|null;total?:number}
interface DuplicateScanComplete {status:string;result?:DuplicateScan;errors?:string[]}
// categories.js: the filename/folder pattern classifier.
interface DuplicateMember {path?:string;filename?:string;file_size?:number}
interface DuplicateCategoryGroup {winner:DuplicateMember;other_kept?:DuplicateMember[];losers?:DuplicateMember[];empty_file_group?:boolean}
interface DuplicateCategorySummary {id:string;label:string;groups:number;extras:number;bytes:number}
interface DuplicateFilterController {init(options:{page:string;root:HTMLElement|null;scopeLabel:string;onChange:()=>unknown}):Promise<void>;getRules():unknown;getVisual?():unknown;hasFilters?():boolean;isMuted():boolean;setResultTotal?(total:number|null):void;setVisualInfo?(info:unknown):void}
interface Window {
  escapeHtml(value:unknown):string;
  formatBytes(value:number):string;
  safeFetch<T>(url:string,options?:RequestInit,settings?:{toast?:boolean}):Promise<T>;
  safeEventSource(url:string,callbacks:{onProgress?(progress:DuplicateScanProgress):void;onComplete?(event:DuplicateScanComplete):void;onError?():void}):EventSource;
  showToast?(message:string,kind?:string):void;
  showRevealFeedback(data:unknown):void;
  openContextMenu(event:MouseEvent,items:Array<{label:string;onClick:()=>void}>):void;
  vireoThumbnailUrl?(photo:DuplicatePhoto):string;
  VIREO_REVEAL_LABEL:string;
  VIREO_FILE_MANAGER_NAME?:string;
  VireoFilter:DuplicateFilterController;
  Vireo:VireoPageHookRegistry;
}
