// Review page payloads, edit state, and shared-script dependencies.
interface ReviewRecipe {rotation?:number;crop?:{x?:number;y?:number;w?:number;h?:number}}
interface ReviewPrediction {card_id?:string;consensus_species?:string;consensus_species_key?:string;species_key?:string}
interface ReviewPrediction {detection_id?:number;evidence?:ReviewPredictionEvidence;flag?:string;id:number;photo_id:number;filename:string;species:string;confidence:number;status:string;model?:string;group_id?:string;labels_fingerprint?:string;timestamp?:string;category?:string;effective_category?:string|null;existing_species?:string[];total_votes:number;vote_count?:number;individual?:string|Record<string,number>;alternatives?:{id:number;species:string;confidence:number}[];box_x?:number;box_y?:number;box_w?:number;box_h?:number;is_species_representative?:boolean;life_list?:{species:string;is_current_photo?:boolean;is_species_representative?:boolean}[];edit_recipe?:ReviewRecipe;quality_score?:number;subject_sharpness?:number;sharpness?:number;box_sharpness?:number|null;width?:number;height?:number;eye_x?:number;eye_y?:number}
interface ReviewGroupState {groupId:string|null;model:string|null;items:ReviewPrediction[];picks:Set<number>;rejects:Set<number>;removed:Set<number>;selected:number|string|null;selectedIds:Set<number|string>;selectionAnchor:number|string|null;upgraded:boolean}
interface ReviewOffsetTarget {card:HTMLElement;photoId:string;origTx:number;origTy:number}
interface ReviewDrag {card:HTMLElement;photoId:string;targets:ReviewOffsetTarget[];startX:number;startY:number;moved:boolean}
interface ReviewAlignDrag {targets:ReviewOffsetTarget[];startX:number;startY:number;moved:boolean}
interface ReviewMenuItem {label?:string;title?:string;disabled?:boolean;disabledHint?:string;onClick?:()=>unknown;separator?:boolean;chips?:ReviewMenuItem[]}
interface ReviewRule {field?:string;rules?:ReviewRule[]}
interface ReviewFilterController {loadExpression(rules:ReviewRule[]|{rules?:ReviewRule[]},visual:unknown):void;getRules():ReviewRule[]|{rules?:ReviewRule[]};getVisual():unknown;setResultTotal(count:number):void;setVisualInfo(info:unknown):void;hasFilters():boolean;init(options:{page:string;root:HTMLElement|null;scopeLabel:string;onChange:()=>unknown;getScope:()=>{folder_id:null;collection_id:number|null}}):Promise<void>}
declare const VireoFilter:ReviewFilterController;
declare const VireoViewPreferences:{restoreAll(element:HTMLElement|null):void};
declare const Vireo:VireoPageHookRegistry&{predictions:{decidedPredictionIds(value:unknown):number[];groupedDecisionTracker():{alreadyDecided(id:number):boolean;record(value:unknown):number[]}}};
declare function safeFetch<T>(url:string,options?:RequestInit,feedback?:{toast?:boolean}):Promise<T>;
declare function showToast(message:string,kind?:string):void;
declare function showRevealFeedback(response:unknown):void;
declare function escapeHtml(value:unknown):string;
declare function escapeAttr(value:unknown):string;
declare function openLightbox(id:number|string,filename:string,photos:{id:number;filename:string}[]):void;
declare function openContextMenu(event:MouseEvent,items:ReviewMenuItem[]):void;
declare function isTauri():boolean;
declare function vireoPreviewUrl(id:number|string,size:number):string;
declare function _lbRecipeHasGeometricEdit(recipe:ReviewRecipe|undefined):boolean;
declare function matchesShortcut(event:KeyboardEvent,shortcut:string):boolean;
declare function formatShortcut(shortcut:string):string;
interface Window {VireoFilter?:ReviewFilterController;VIREO_REVEAL_LABEL:string;_vireoShortcuts:Record<string,{accept?:string;skip?:string}>;vireoRememberPhotoEditRecipe?(id:number,recipe:ReviewRecipe|undefined,options:{skipIfLocallyWritten:boolean}):void;vireoThumbnailUrl?(photo:ReviewPrediction|number|string):string;vireoPhotoHasOrientationEdit?(id:number):boolean;buildSpeciesHighlightMenuItems?(ids:number[],options:{showFetchFallback:boolean}):ReviewMenuItem[];buildSpeciesRepresentativeMenuItems?(ids:number[],options:{showFetchFallback:boolean}):ReviewMenuItem[];buildOpenInEditorMenuItems?(ids:number[]):ReviewMenuItem[];submitToInat?(id:number):void;vireoEditNav?:{setList(ids:number[],current:number):void;setLastPhoto(id:number):void};developPhotos?(ids:number[]):void;openInBrowse(id:number):void}
interface DocumentEventMap {'lightbox:renderchanged':CustomEvent<{photoIds?:number[]}>;'lifelist:changed':CustomEvent<{species?:string;photoId?:number}>;'vireo:edit-history-changed':CustomEvent<unknown>;'vireo:photo-render-changed':CustomEvent<{photo_ids?:number[]}>}

declare function checkPendingSync():void;

interface ReviewQueueItemState {inspected:boolean;unresolved:boolean;subject:number|null;center:[number,number]|null}
interface ReviewQueueSession {photoId:number|null;items:Record<string,ReviewQueueItemState>;followUpOnly:boolean}
interface ReviewQueuePhoto {id:number;flag?:string;flag_note?:{description?:string}|null}
interface ReviewQueueSubject {id:number;box_x:number;box_y:number;box_w:number;box_h:number;is_primary?:boolean;analysis?:unknown}
interface ReviewQueueState {savedRules:ReviewRule[]|{rules?:ReviewRule[]}|null;savedVisual:unknown;workspace:number|null;key:string;session:ReviewQueueSession;enabled:boolean;items:ReviewPrediction[];current:ReviewQueuePhoto|null;subjects:ReviewQueueSubject[];sequence:number;cropSequence:number;source:string;manual:boolean;busy:boolean;storageError:boolean}

interface ReviewPredictionEvidence {
  score:{model:string;kind:string;value:number;label_count:number|null;raw_value:number|null;raw_kind:string|null;calibrated_correctness:boolean;explanation:string};
  subject:{decision:string;revision:number;detector_model:string;detector_score:number|null;category:string|null;explanation:string};
  disagreement:boolean;model_candidates:{model:string;species:string;relative_score:number}[];
  match:{state:string;explanation:string};
  geography:{state:string;latitude?:number;longitude?:number;source?:string;observations_url?:string;explanation:string};reasons:string[];
}
