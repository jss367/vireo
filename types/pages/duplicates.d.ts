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
interface Window {vireoThumbnailUrl?:(photo:DuplicatePhoto)=>string;VIREO_REVEAL_LABEL:string;VIREO_FILE_MANAGER_NAME?:string}
declare function safeFetch<T>(url:string,options?:RequestInit,settings?:{toast?:boolean}):Promise<T>;
declare function escapeHtml(value:string):string;
declare function formatBytes(value:number):string;
declare function showToast(message:string,kind:string):void;
interface DuplicateScanProgress {phase?:string;current_file?:string;current?:number|null;total?:number}
interface DuplicateScanComplete {status:string;result?:DuplicateScan;errors?:string[]}
declare function safeEventSource(url:string,callbacks:{onProgress?(progress:DuplicateScanProgress):void;onComplete?(event:DuplicateScanComplete):void;onError?():void}):EventSource;
declare function showRevealFeedback(data:unknown):void;
declare function openContextMenu(event:MouseEvent,items:Array<{label:string;onClick:()=>void}>):void;
declare var Vireo: VireoPageHookRegistry;
declare var VireoFilter: {init(options:{page:string;root:HTMLElement|null;scopeLabel:string;onChange:()=>unknown}):Promise<void>;getRules():unknown;getVisual?():unknown;hasFilters?():boolean;isMuted():boolean;setResultTotal?(total:number|null):void;setVisualInfo?(info:unknown):void};
interface Window {VireoFilter?:typeof VireoFilter}
declare var DuplicateCategories:{classify(group:DuplicateProposal):string;label(id:string):string;summarize(groups:DuplicateProposal[]):DuplicateCategory[]};
