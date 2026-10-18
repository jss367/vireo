// Location Review API and the Google Maps / Leaflet subset used by this page.
interface LocationPoint {lat:number;lng:number}
interface LocationPhoto {id:number;filename:string;latitude:number;longitude:number;timestamp?:string|null;distance_m:number;fingerprint?:string;assigned?:false;sidecar_location?:{latitude?:number;longitude?:number}}
interface LocationGroup {id:string;photos:LocationPhoto[];photo_ids:number[];count:number;center?:LocationPoint;captured_from?:string|null;captured_to?:string|null;spread_m:number;bounds?:{south:number;west:number;north:number;east:number};assigned_location?:{name:string;latitude:number;longitude:number}}
interface LocationAddressComponent {name?:string;long_name?:string;short_name?:string;types?:string[]}
interface LocationPlace {id?:string;place_id?:string;name?:string;displayName?:string|{text?:string};types?:string[];geometry?:{location?:LocationGooglePoint;viewport?:google.maps.LatLngBounds};location?:LocationGooglePoint;vicinity?:string;formatted_address?:string;formattedAddress?:string;address_components?:LocationAddressComponent[]}
interface LocationGooglePoint {lat:number|(()=>number);lng:number|(()=>number)}
interface LocationChoice {kind:string;name:string;place_id?:string;keyword_id?:number;types?:string[];latitude?:number;longitude?:number;category?:string;type_label?:string;place_type?:string;area_scope?:string;previously_used?:boolean;category_is_fallback?:boolean;photo_count?:number;distance_m?:number;relationship?:string;context?:string;address_components?:LocationAddressComponent[];_googleResult?:LocationPlace}
interface LocationSaved extends Omit<LocationChoice,'kind'> {id:number;type?:string}
interface LocationCollection {id:number;name:string;available_photo_count?:number;photo_count?:number;offline_photo_count?:number;count_error?:string;has_visual?:boolean}
interface LocationSource {scope?:string;collection_id?:number;photo_ids?:number[]}
interface LocationAssignment {group:LocationGroup;choiceKey:string;completed:number;total:number;photoIds:number[];processedIds:Set<number>}
interface LocationConfig {google_maps_api_key?:string;google_maps_prefer_english?:boolean;write_assigned_location_to_xmp?:boolean}
interface LocationState {source:LocationSource|null;mode:string;minimumDistance:number;includeKept:boolean;selectedGpsPhotos:Set<number>;queuedCorrections:number;gapMinutes:number;inspectAll:boolean;photoPage:number;savedLocations:LocationSaved[];returnUrl:string;collections:LocationCollection[];groups:LocationGroup[];currentIndex:number;initialGroupCount:number;assignedGroups:number;skippedGroups:LocationGroup[];selectedChoice:LocationChoice|null;includeCoordinates:boolean;savedCandidates:LocationChoice[];googleCandidates:LocationChoice[];googleCandidatesLoaded:boolean;regionCandidates:LocationChoice[];searchedCandidate:LocationChoice|null;suggestionMode:string;config:LocationConfig|null;mapMode:string|null;map:google.maps.Map|L.Map|null;photoMarkers:(google.maps.Marker|L.Marker)[];candidateMarkers:(google.maps.Marker|L.Marker)[];placesService:google.maps.places.PlacesService|null;autocomplete:google.maps.places.Autocomplete|null;googleMapsUnavailableReason:string|null;renderToken:number;assignment:LocationAssignment|null;isAssigning:boolean}
interface LocationPreview {groups?:LocationGroup[];reviewable?:number;skipped?:{reason:string}[];unresolved?:unknown[];total?:number}
interface LocationPlaceSubmit {place_id?:string;name:string;types:string[];lat?:number;lng?:number;address_components:{name:string;short_name:string;types:string[]}[]}
interface LocationAssignmentBody {photo_ids:number[];keyword_id?:number;name?:string;latitude?:number;longitude?:number;place_id?:string;place?:LocationPlaceSubmit}
declare function safeFetch<T>(url:string,options?:RequestInit,feedback?:{toast?:boolean}):Promise<T>;
declare function showToast(message:string,kind?:string):void;
declare function escapeHtml(value:unknown):string;
declare function escapeAttr(value:unknown):string;
declare function openLightbox(id:number,filename:string,photos:LocationPhoto[]):void;
declare function closeLightbox():void;
declare function checkPendingSync():void;
interface Window {google?:typeof google;L?:typeof L;_vireoLocationReviewMapsReady():void;_vireoLeafletPromise?:Promise<typeof L | undefined>;}
interface DocumentEventMap {'lightbox:photodeleted':CustomEvent<{photoId?:number}>}
declare namespace google.maps {
 class LatLng {constructor(lat:number,lng:number);lat():number;lng():number}
 class LatLngBounds {extend(point:LocationPoint):void}
 class Map {constructor(element:HTMLElement,options:{center:LocationPoint;zoom:number;mapTypeId:string;mapTypeControl:boolean;mapTypeControlOptions:{mapTypeIds:string[];style:string};streetViewControl:boolean;fullscreenControl:boolean;clickableIcons:boolean});fitBounds(bounds:LatLngBounds,padding?:number):void;getZoom():number;setZoom(zoom:number):void;panTo(point:{lat:number;lng:number|undefined}):void;addListener(name:string,callback:(event:{placeId?:string;stop?():void})=>void):void}
 class Marker {constructor(options:{position:{lat:number;lng:number};map:Map|null;title:string;icon?:{path:string;scale:number;fillColor:string;fillOpacity:number;strokeColor:string;strokeWeight:number};zIndex:number;label?:{text:string;color:string;fontWeight:string}|null;opacity?:number});setMap(map:Map|null):void;addListener(name:string,callback:()=>void):void}
 const SymbolPath:{CIRCLE:string};const MapTypeControlStyle:{HORIZONTAL_BAR:string};const event:{addListenerOnce(map:Map,name:string,callback:()=>void):void};
 class Geocoder {geocode(options:{location:LatLng},callback:(results:LocationPlace[]|null,status:string)=>void):void}
 namespace places {
  interface SearchRequest {location:LatLng;radius:number;type?:string;keyword?:string}
  class PlacesService {constructor(map:Map);getDetails(request:{placeId:string|undefined;fields:string[]},callback:(place:LocationPlace|null,status:string)=>void):void;nearbySearch(request:SearchRequest,callback:(results:LocationPlace[]|null,status:string)=>void):void}
  const PlacesServiceStatus:{OK:string};
  class Autocomplete {constructor(input:HTMLInputElement,options:{fields:string[];types:string[]});bindTo(name:string,map:Map|null):void;addListener(name:string,callback:()=>void):void;getPlace():LocationPlace}
 }
}
declare namespace L {
 class Map {removeLayer(marker:Marker):void;fitBounds(points:[number,number][],options:{padding:[number,number];maxZoom:number}):void;setView(point:[number,number],zoom:number):this;invalidateSize():void}
 class Marker {addTo(map:Map):this;bindTooltip(name:string):this;on(name:string,callback:()=>void):void}
 interface TileLayer {}
 function marker(point:[number,number],options:{icon?:unknown;title:string}):Marker;
 function divIcon(options:{className:string;html:string;iconSize:[number,number];iconAnchor:[number,number]}):unknown;
 function tileLayer(url:string,options:{attribution:string;maxZoom:number}):TileLayer;
 function map(id:string,options:{layers:TileLayer[]}):Map;
 const control:{layers(layers:Record<string,TileLayer>):{addTo(map:Map):void}};
}
// The page-hook registry from vireo-page-hooks.js (types/page-hooks.d.ts).
declare var Vireo: VireoPageHookRegistry;
