// Photo Editor domain models and contracts of separately checked shared scripts.
type EditorNumericKey = 'exposure'|'highlights'|'shadows'|'whites'|'blacks'|'contrast'|'texture'|'clarity'|'dehaze'|'temperature'|'tint'|'vibrance'|'saturation'|'sharpen'|'sharpen_radius'|'noise_reduction'|'distortion'|'vignetting'|'defringe_purple'|'defringe_green';
type EditorGradeZone = 'shadows'|'midtones'|'highlights';
interface EditorCrop {x:number;y:number;w:number;h:number}
interface EditorPoint {x:number;y:number}
interface EditorFlip {horizontal?:boolean;vertical?:boolean}
interface EditorMask {ref:string;source_digest:string;corrected?:boolean;feather?:number}
interface EditorLocalRegion {region:'subject'|'background';adjustments:Record<string,number>}
type EditorColorGrading = Partial<Record<EditorGradeZone,Record<string,number>>> & {balance?:number};
type EditorAdjustments = Partial<Record<EditorNumericKey,number>> & {white_balance?:Record<string,number>;tone_curve?:Record<string,number>;hsl?:Record<string,Record<string,number>>;color_grading?:EditorColorGrading;denoise_mode?:string;lens_profile?:boolean;point_curves?:Record<string,[number,number][]>;point_color?:EditorPointColor[]};
interface EditorRecipe {version?:number;rotation?:number;straighten?:number;flip?:EditorFlip;crop?:EditorCrop;adjustments?:EditorAdjustments;local?:{mask:EditorMask;regions:EditorLocalRegion[];background_denoise?:number}}
interface EditorLensProfile {matched:boolean;lens:string;message:string;warp:number[]|null;vignette:number[]|null}
interface EditorPhoto {lens_profile?:EditorLensProfile;id:number;filename:string;width?:number;height?:number;timestamp?:string;species?:string[];rating?:number;folder_name?:string;metadata?:unknown;edit_recipe?:EditorRecipe;denoise_profile?:{camera_model?:string;camera_make?:string;iso?:number;source?:string;match?:string;profile_iso?:number[]}}
interface EditorPreset {id:number;name:string;recipe?:EditorRecipe;fields?:string[]}
interface EditorLocalDraft {subject:Record<string,number>;background:Record<string,number>;feather:number;backgroundDenoise:number}
interface EditorVersion {id:string;name:string;kind:"copy"|"snapshot";recipe:EditorRecipe|null;revision:string;created_at:string;updated_at:string}
interface EditorState {versionId:string|null;version:EditorVersion|null;photoId:number|null;photo:EditorPhoto|null;savedRecipe:EditorRecipe;savedRecipeSeq:number;recipe:EditorRecipe;presets:EditorPreset[];localDraft:EditorLocalDraft;localMask:EditorMask|null;localMaskPromise:Promise<EditorMask>|null;localMaskUpdateSeq:number;localAvailable:boolean;localStale:boolean;savedLocalStale:boolean;maskOverlay:boolean;maskOverlaySeq:number;maskOverlayTimer:ReturnType<typeof setTimeout>|null;previewSeq:number;previewTimer:ReturnType<typeof setTimeout>|null;previewRefineTimer:ReturnType<typeof setTimeout>|null;previewInputAt:number|null;previewTimings:{interactive:boolean;requestedSize:number;elapsedMs:number}[];drag:{handle:string;startX:number;startY:number;crop:EditorCrop;imgW:number;imgH:number}|null;pan:{pointerId:number;startX:number;startY:number;scrollLeft:number;scrollTop:number}|null;spacePan:boolean;showBefore:boolean;cropEditing:boolean;zoomMode:string;zoomPercent:number;cropAspect:number|null;navIds:number[];baseNavIds:number[];loadSeq:number;loading:boolean;histogramTimer:ReturnType<typeof setTimeout>|null;searchTimer:ReturnType<typeof setTimeout>|null;searchSeq:number;searchQuery:string;searchQuerySeq:number;savingPhotoIds:Record<string,boolean>;suppressUnloadPrompt:boolean}
interface EditorPreviewRequest {url:string;size:number;seq:number;interactive:boolean;started:number;image?:HTMLImageElement;timeout?:ReturnType<typeof setTimeout>;serverSequence?:number}
interface EditorCropRatio {enabled:boolean;aspect:number|null;revision?:number}
interface EditorPendingCropRatio extends EditorCropRatio {revision:number}
interface EditorBrushOptics {distortion:number;warp:number[]|null}
interface EditorMaskStroke {pointerId:number;mode:string;rect:DOMRect;recipe:EditorRecipe;applyCrop:boolean;native:{width:number;height:number};radius:number;screenSize:number;points:[number,number][];softness:number;strength:number;optics:EditorBrushOptics|null;lastScreen?:[number,number]}
interface EditorAutoTone {style?:string;notes?:string[];metering?:string;subject_source?:string;adjustments?:Partial<Record<EditorNumericKey,number>>}
interface EditorExportRequest {version_id?:string;version_revision?:string;photo_ids:number[];destination:string;export_to_subfolder:boolean;subfolder_name:string;naming_template:string;max_size:number|null;format:string;quality?:number;metadata_fields?:string[];reveal_after_export?:boolean}
interface EditorExportPreflight {error?:string}
interface EditorMenuItem {label?:string;separator?:boolean;disabled?:boolean;disabledHint?:string;onClick?:()=>unknown}
declare function safeFetch<T>(url:string,options?:RequestInit,feedback?:{toast?:boolean}):Promise<T>;
declare function showToast(message:string,kind?:string):void;
declare function openContextMenu(event:MouseEvent,items:EditorMenuItem[]):void;
declare function isTauri():boolean;
declare function pickDirectory(title:string,options:{defaultPath?:string}):Promise<string|string[]|null>;
declare const VireoViewPreferences:{read(key:string):string|null;write(key:string,value:string):void};
declare const VireoBatchEdits:{fields():Promise<{path:string}[]>;selectFields(options:{title:string;fields?:string[];available?:string[];action?:string;hint?:string}):Promise<string[]|null>;copy(photoId:number,recipe:EditorRecipe):Promise<void>};
declare const VireoExportPresets:{markCustom():void;modalOpened():void;subfolderName():string;init(options:unknown):void};
declare const VireoExportCollisions:{schedule():void;reset():void;cancelPending():void;acknowledged(preflight:EditorExportPreflight):boolean;init(options:unknown):void};
declare const VireoExportJob:{watch(jobId:string):void};
declare class VireoFolderBrowser {constructor(options:{overlayId:string;defaultMode:string;modes:Record<string,{title:string;multiple:boolean;showCounts:boolean;startPath:()=>string;onSelect:(path:string)=>void}>});sequence:number;open(mode:string,options?:{skipInitialBrowse:boolean}):void}
interface Window {openContextMenu?:typeof openContextMenu;renderHistoryControls?():void;setEditorSpacePan(active:boolean):void;submitToInat?(id:number):void;vireoEditNav?:{getList():number[];getLastPhoto():number|null;setList(ids:number[],current:number|null|undefined):void;setLastPhoto(id:number):void;setCopiedRecipe(recipe:EditorRecipe,metadata:{source:string|null;at:number}):Promise<void>};vireoRefreshEditRecipeCache?(updates:Record<string,EditorRecipe|null>):void;vireoRefreshNavigationHrefs?():void}
interface DocumentEventMap {'vireo:edit-history-busy':CustomEvent<{busy:boolean}>}

type EditorPointColorKey = 'hue_range'|'saturation_range'|'luminance_range'|'hue'|'saturation'|'luminance';
type EditorPointColor = {sample:[number,number,number]} & Partial<Record<EditorPointColorKey,number>>;
interface EditorHistorySnapshot {recipe:EditorRecipe;cropAspect:number|null;cropEditing:boolean;localStale:boolean}
interface EditorHistoryGesture {pointerId:number;undoBefore?:EditorHistorySnapshot[];redoBefore?:EditorHistorySnapshot[]}
interface Window {doUndo():void;doRedo():void}
// The page-hook registry from vireo-page-hooks.js (types/page-hooks.d.ts).
declare var Vireo: VireoPageHookRegistry;
