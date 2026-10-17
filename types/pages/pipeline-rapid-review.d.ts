// Rapid Review's classic scripts share this page-specific program.
interface RapidPhoto {id:number; filename?:string; flag?:string; label?:string; confirmed_species?:string|null; confirmed_species_list?:string[]; confirmed_species_identities?:{name:string;key:string}[]; quality_composite?:number|null; subject_tenengrad?:number|null;}
interface RapidOverride {species?:string; species_list?:string[]; confirmed?:boolean;}
interface RapidObjectBurst {photo_ids:number[]; species_override?:RapidOverride|null;}
// load_results preserves legacy ID arrays alongside modern burst objects.
type RapidBurst = RapidObjectBurst | number[];
interface RapidEncounter {photo_ids?:number[]; bursts?:RapidBurst[]; species?:string[]; confirmed_species?:string|null; confirmed_species_list?:string[];}
interface RapidSummary {keep_count?:number; review_count?:number; reject_count?:number;}
interface RapidResults {photos?:RapidPhoto[]; encounters?:RapidEncounter[]; summary?:RapidSummary; species_identities?:Record<string,{key:string}>;}
interface RapidSession {rawIndex:number;encIdx:number;burstIdx:number;ids:number[];species:string;speciesList:string[];hasBurstOverride:boolean;title:string;subtitle:string;}
type RapidZone = 'pick'|'reject'|'review';
interface RapidHistory {id:number;prevZone:RapidZone;prevReviewed:boolean;prevTouched:boolean;prevIndex:number;}
interface RapidQueue {filter:string;includeRejected:boolean;multiOnly:boolean;}
interface RapidKeywordRow {flag?:string;has_species_keyword?:boolean;has_species_keywords?:Record<string,boolean>;}
interface RapidGroupResponse {photos?:Record<string,RapidKeywordRow>;}
interface RapidSpeciesResponse {species_list?:string[];encounters?:RapidEncounter[];summary?:RapidSummary;}
interface RapidSpeciesRequest {species:string;remove?:boolean;add?:boolean;previous_species?:string;}
interface RapidApplyDiff {flagNew:number;rejectNew:number;clearNew:number;tagNew:number;speciesChanged:boolean;species:string;speciesList:string[];removedSpecies:string[];}
interface RapidState {results:RapidResults|null;photoMap:Record<number,RapidPhoto>;allSessions:RapidSession[];sessions:RapidSession[];sessionIndex:number;items:RapidPhoto[];picks:Set<number>;rejects:Set<number>;reviewed:Set<number>;touched:Set<number>;initialFlags:Record<number,string>;initialSpeciesKeywords:Record<number,Record<string,boolean>>;keywordStateSpecies:string;initialSpecies:string;history:RapidHistory[];currentIndex:number;seeded:boolean;applying:boolean;loadError:boolean;historyRefreshFailed:boolean;zoomed:boolean;zoomPhotoId:number|null;zoomAnchor:{x:number;y:number}|null;zoomDrag:{pointerId:number;startX:number;startY:number;scrollLeft:number;scrollTop:number;moved:boolean}|null;suppressZoomClick:boolean;openToken:number;queue:RapidQueue;queuePromptResolve:((choice:string|null)=>void)|null;}
interface RapidShortcutConfig {keyboard_shortcuts?:Record<string,Record<string,string>>;}
interface RapidKeymap {setScope(scope:string):void;matchesShortcut?(event:KeyboardEvent,shortcut:string):boolean;register?(scope:string,item:{key:string;name:string;description:string;category:string;action:()=>boolean}):void;}
interface Window {Keymap:RapidKeymap;_vireoShortcuts?:Record<string,Record<string,string>>;beforeHistoryChange:()=>boolean;afterHistoryChange:()=>Promise<void>;vireoThumbnailUrl?:(photo:RapidPhoto|number)=>string;}
declare var Keymap:RapidKeymap;
declare function matchesShortcut(event:KeyboardEvent,shortcut:string):boolean;
declare function showToast(message:string,kind?:string):void;
declare function escapeHtml(value:unknown):string;
declare function escapeAttr(value:unknown):string;
declare function safeFetch(url:'/api/config',init?:RequestInit,options?:{toast?:boolean}):Promise<RapidShortcutConfig>;
declare function safeFetch(url:'/api/pipeline/results',init?:RequestInit,options?:{toast?:boolean}):Promise<RapidResults>;
declare function safeFetch(url:'/api/pipeline/group/state'|'/api/pipeline/group/apply',init?:RequestInit,options?:{toast?:boolean}):Promise<RapidGroupResponse>;
declare function safeFetch(url:'/api/encounters/species',init?:RequestInit,options?:{toast?:boolean}):Promise<RapidSpeciesResponse>;
declare function safeFetch(url:'/api/pipeline/save-cache',init?:RequestInit,options?:{toast?:boolean}):Promise<unknown>;
// WebIDL coerces assigned numeric counts to text; the implementation relies on that setter.
interface RapidTextOutput {textContent:string|number|null;}
