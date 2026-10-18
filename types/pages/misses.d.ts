// Payloads used by the Misses page; shared implementations are checked separately.
type MissCategory = 'no_subject' | 'clipped' | 'oof';
interface MissPhoto {id:number;filename?:string;burst_id?:string|number;detection_box?:string|MissBox|null;detector_confidence_threshold?:number;subject_size?:number;subject_tenengrad?:number;bg_tenengrad?:number;detection_conf?:number;raw_detection_conf?:number;is_species_representative?:boolean;life_list?:{species:string;is_current_photo?:boolean;is_species_representative?:boolean}[];companion_path?:string}
interface MissBox {x:number;y:number;w:number;h:number}
interface MissConfig {detector_confidence?:number;miss_det_confidence?:number;miss_classifier_override_conf?:number;miss_bbox_area_min?:number;miss_oof_ratio?:number}
interface MissPayload {no_subject?:MissPhoto[];clipped?:MissPhoto[];oof?:MissPhoto[];config?:MissConfig;visual?:unknown;saved_defaults?:boolean;updated?:number}
interface MissRule {field?:string;op?:string;value?:unknown;mode?:string;rules?:MissRule[]}
interface MissRuleRoot {mode:string;rules:MissRule[]}
interface MissFilters {rules:MissRuleRoot|MissRule[];visual:unknown}
interface MissBody extends MissConfig {rules?:MissRuleRoot|MissRule[];visual?:unknown;since?:string;save_defaults?:boolean;category?:MissCategory}
interface MissMenuItem {label?:string;onClick?:()=>unknown;separator?:boolean;disabled?:boolean}
interface MissFilterController {getRules():MissFilters['rules'];getUserRules():MissFilters['rules'];getVisual():unknown;setResultTotal(total:number):void;setVisualInfo(info:unknown):void;loadExpression(rules:MissRuleRoot,visual:unknown,options:{reason:string}):void;init(options:{page:string;root:HTMLElement|null;scopeLabel:string;excludedValues:{flag:string[]};onChange:()=>void}):Promise<void>}
declare const VireoFilter:MissFilterController;
declare function escapeHtml(value:unknown):string;
declare function escapeAttr(value:unknown):string;
declare function showToast(message:string,kind?:string):void;
declare function matchesShortcut(event:KeyboardEvent,shortcut:string):boolean;
declare function preferredScrollBehavior():ScrollBehavior;
declare function requestLightboxFullscreen():void;
declare function openContextMenu(event:MouseEvent,items:MissMenuItem[]):void;
declare function openLightbox(id:number,filename:string,photos:{id:number;filename?:string;missCategory:MissCategory}[]):void;
declare function showDeleteDialog(ids:number[],companions:number,callback:(data:{failed_photo_ids?:number[]})=>void):void;
interface Window {VireoFilter?:MissFilterController;vireoThumbnailUrl?(photo:MissPhoto):string;_vireoShortcuts?:{browse?:{flag?:string;reject?:string;unflag?:string}};_lightboxZoomHandled:boolean;afterHistoryChange():Promise<void>;loadMisses():Promise<void>;getActiveSelection():number[];openBrowseShortcutPhoto(fullscreen:boolean):boolean;handleMissesLightboxFlagShortcut(photoId:number,flag:string):boolean;buildSpeciesHighlightMenuItems?(ids:number[],options:{showFetchFallback:boolean}):MissMenuItem[];buildSpeciesRepresentativeMenuItems?(ids:number[],options:{showFetchFallback:boolean}):MissMenuItem[]}
interface DocumentEventMap {'lifelist:changed':CustomEvent<{species?:string;photoId?:number}>;'lightbox:flagchanged':CustomEvent<{photoId?:number;flag?:string}>;'lightbox:photochanged':CustomEvent<{photoId?:number}>;'lightbox:closed':CustomEvent<void>}
