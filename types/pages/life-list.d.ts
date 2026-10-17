// Life List API models and contracts of shared scripts loaded by this page.
interface LifePhoto {id:number;filename:string;flag?:string;is_species_representative?:boolean}
interface LifeSpecies {species:string;scientific_name?:string;common_name?:string;photo_count:number;number:number;first_seen?:string;last_seen?:string;locations?:string[];photos:LifePhoto[];best?:LifePhoto;has_more?:boolean;loading_more?:boolean;loaded_count?:number;taxonomic_class?:LifeTaxon;taxon_rank?:string}
interface LifeData {species:LifeSpecies[];meta:{species_count:number;photo_count:number}}
interface LifeTaxon {id:number;name:string;common_name?:string}
interface LifeNode extends LifeTaxon {rank:string;child_rank?:string;found_children?:number;total_children?:number;children?:LifeNode[];found_species:number;total_species:number;found?:boolean;order?:string;photo?:LifePhoto}
interface LifeCrumb {id:number;name:string;rank:string}
interface LifeCount {found:number;total:number}
interface LifeUncounted {name:string;reason?:string;taxon_rank?:string;photo_count:number;filter_token:string}
interface LifeExplorer {taxonomy_ready?:boolean;classes?:LifeTaxon[];root?:LifeTaxon;nodes?:LifeNode[];summary?:Record<string,LifeCount>;uncounted_identifications?:{scoped_count?:number;workspace_count?:number;scoped?:LifeUncounted[];workspace?:LifeUncounted[]}}
interface LifeRankData {found?:number;total?:number;items?:LifeNode[]}
interface LifeRankView {rank:string;data:LifeRankData;search:string;missingOnly:boolean;showAll:boolean}
interface LifePublishOptions {include_life_list:boolean;photos_per_species:number;include_highlights:boolean;limit_per_bucket:number;include_locations:boolean;max_size:string;destination?:string}
interface LifePublishPreflight {life_list_species:number;highlight_buckets:number;unidentified_photos?:number;image_count:number;data_file_count:number}
declare function escapeHtml(value:unknown):string;
declare function escapeAttr(value:unknown):string;
declare function showToast(message:string,kind?:string):void;
declare function safeFetch<T>(url:string,options?:RequestInit,feedback?:{toast?:boolean}):Promise<T>;
declare function safeEventSource(url:string,callbacks:{onComplete:()=>void}):unknown;
declare function openLightbox(id:number,filename:string,photos:LifePhoto[]):void;
declare function isTauri():boolean;
declare function pickDirectory(title:string,options:{defaultPath?:string}):Promise<string|string[]|null>;
declare const VireoViewPreferences:{restoreAll(element:HTMLElement|null):void};
declare const VireoTextSearch:{readOptions(prefix:string):{caseSensitive?:boolean;wholeWord?:boolean;regex?:boolean};matchesFields(fields:string[],query:string,options:ReturnType<typeof VireoTextSearch.readOptions>):boolean};
declare class VireoFolderBrowser {constructor(options:{overlayId:string;defaultMode:string;modes:Record<string,{title:string;multiple:boolean;showCounts:boolean;startPath:()=>string;onSelect:(path:string)=>void}>});sequence:number;open(mode:string,options?:{skipInitialBrowse:boolean}):void}
interface Window {afterHistoryChange():void;vireoThumbnailUrl?(photo:LifePhoto):string;openLightbox?:typeof openLightbox;vireoLightboxSession:{requestedPhotoId():number|null}}
interface HTMLElement {_llSummaryWired?:boolean}
interface DocumentEventMap {'lightbox:photochanged':CustomEvent<{photoId?:number}>;'lightbox:navigationboundary':CustomEvent<{delta?:number;photoId?:number}>;'lightbox:flagchanged':CustomEvent<{previousFlag?:string;flag?:string}>;'lightbox:closed':CustomEvent<void>;'lifelist:changed':CustomEvent<unknown>}
