interface KeywordRecord {id:number;name:string;type?:string;parent_id?:number|null;path?:string[];paths?:string[][];taxon_common_name?:string|null;taxon_name?:string|null;taxon_id?:number|null;source_taxon_id?:number|null;photo_count?:number;direct_photo_count?:number;latitude?:number|null;longitude?:number|null;place_id?:string|null;members?:KeywordRecord[]}
interface KeywordIdentity extends KeywordRecord {identity:string;members:KeywordRecord[]}
interface KeywordMergeRecord extends KeywordRecord {path:string[];type:string;parent_id:number|null;taxon_id:number|null;source_taxon_id:number|null;place_id:string|null}
interface KeywordSpeciesOption {taxon_common_name?:string|null;taxon_name?:string|null;source_taxon_id:number|null;taxon_id:number|null;name:string}
interface KeywordMergeOptions {name:Array<{value:string}>;parent:Array<{parent_id:number|null;path:string[]}>;type:Array<{value:string}>;place:Array<{place_id:string;name:string;path:string[];latitude?:number|null;longitude?:number|null}>;species:KeywordSpeciesOption[];coordinates:Array<{latitude:number;longitude:number;name:string}>}
interface KeywordMergeChild {name:string;outcome:string;to_path?:string[];photo_count?:number}
interface KeywordMergePreview {error?:string;options:KeywordMergeOptions;resolved:KeywordMergeRecord;requires_choice:string[];combined_count:number;removed_count:number;children:KeywordMergeChild[];notes?:string[];target:KeywordMergeRecord;sources:KeywordMergeRecord[];preview_token?:string|null}
interface KeywordMergeOverrides {name?:string;parent_id?:number|null;type?:string;place_id?:string|null;coordinates?:{latitude:number|null;longitude:number|null}|null;species?:{taxon_id:number|null;source_taxon_id:number|null}|null}
interface KeywordMutation {error?:string;message?:string;merged?:boolean;keyword?:KeywordRecord}
interface KeywordLocationMatch {source_path:string[];source_count:number;target_path:string[];name:string;place_id:string;combined_count:number;conflicting_photo_count:number;source_id:number;target_id:number}
interface KeywordLeafletMap {remove():void;invalidateSize():void;fitBounds(bounds:unknown,options:{padding:[number,number];maxZoom:number}):void}
interface KeywordLeafletLayer {addTo(map:KeywordLeafletMap):KeywordLeafletLayer;remove():void;bindTooltip(text:string):KeywordLeafletLayer}
interface KeywordLeaflet {map(element:HTMLElement,options:{attributionControl:boolean}):KeywordLeafletMap;tileLayer(url:string,options:{attribution:string;maxZoom:number}):KeywordLeafletLayer;layerGroup(layers:KeywordLeafletLayer[]):KeywordLeafletLayer;marker(point:[number,number],options:{title:string;zIndexOffset:number}):KeywordLeafletLayer;circleMarker(point:[number,number],options:{radius:number;color:string;weight:number;fillOpacity:number}):KeywordLeafletLayer;latLngBounds(points:[number,number][]):unknown}
interface KeywordGooglePlace {place_id?:string;name?:string;formatted_address?:string;types?:string[];geometry?:{location?:{lat:number|(()=>number);lng:number|(()=>number)}};address_components?:Array<{long_name?:string;name?:string;short_name?:string;types?:string[]}>}
interface KeywordPlaceSubmission {place_id:string;name:string;types:string[];lat:number;lng:number;address_components:Array<{name:string;short_name:string;types:string[]}>}
interface KeywordAutocomplete {addListener(name:'place_changed',listener:()=>void):void;getPlace():KeywordGooglePlace}
interface KeywordGoogle {maps:{places:{Autocomplete:new(input:HTMLInputElement,options:{types:string[];fields:string[]})=>KeywordAutocomplete}}}
interface KeywordMenuItem {label?:string;title?:string;disabled?:boolean;disabledHint?:string;onClick?:()=>void;separator?:boolean;chips?:KeywordMenuItem[]}
// The shared globals the page's shared.js reads, and the Maps callback name
// it sets for the Google Maps script.
interface Window {
  L?:KeywordLeaflet;
  _gmapsCallback?:()=>void;
  google?:KeywordGoogle;
  VireoTextSearch:{readOptions(prefix:string):{matchCase?:boolean;wholeWord?:boolean};matchesFields(fields:string,query:string,options:{matchCase?:boolean;wholeWord?:boolean}):boolean;toggle(prefix:string,option:string,callback:()=>unknown):void};
  escapeHtml(value:unknown):string;
  escapeAttr(value:unknown):string;
  showToast?(message:string,type?:string):void;
  openExternalLink(event:Event,url:string):boolean;
  openContextMenu(event:MouseEvent,items:KeywordMenuItem[]):void;
  coerceSelectionOnContext(ids:Set<number>,id:number):number[];
  openLinkPlaceFromUrl(keywords:KeywordRecord[],open:(id:number,name:string)=>Promise<void>):void;
}
