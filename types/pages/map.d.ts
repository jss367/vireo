// The Map page (map-page.js): the parts of Leaflet and Leaflet.markercluster
// it uses, the photos /api/photos/geo returns, and the functions the other
// map scripts and the shared navbar give it.

interface MapPhoto {
  id: number;
  latitude: number;
  longitude: number;
  filename?: string;
  species?: string | null;
  timestamp?: string | null;
  rating?: number | null;
  coord_source?: string | null;
  keyword_location_name?: string | null;
}
interface MapPageUnplottableFocus {
  id?: number; filename?: string; reason?: string;
  location_keywords?: {id: number; name: string}[];
}
interface MapPageResponse {
  photos: MapPhoto[];
  selection?: unknown;
  total_filtered: number;
  visual?: unknown;
  unplottable_focus?: MapPageUnplottableFocus | null;
}

declare namespace L {
  type LatLngTuple = [number, number];
  interface LatLng { lat: number; lng: number }
  interface Point { x: number; y: number }
  interface LatLngBounds {}
  interface Layer {}
  interface TileLayer extends Layer {}
  interface Icon {}
  interface Marker extends Layer {
    bindPopup(html: string): this;
    openPopup(): this;
    getLatLng(): LatLng;
    on(event: string, handler: () => void): this;
    _photoData?: MapPhoto;
  }
  interface MarkerCluster extends Layer {
    getAllChildMarkers(): Marker[];
    getLatLng(): LatLng;
  }
  interface MarkerClusterEvent { layer: MarkerCluster }
  interface MarkerClusterGroup extends Layer {
    addLayer(marker: Marker): this;
    addLayers?(markers: Marker[]): this;
    clearLayers(): this;
    getBounds(): LatLngBounds;
    zoomToShowLayer(marker: Marker, callback: () => void): void;
    on(event: string, handler: (event: MarkerClusterEvent) => void): this;
  }
  interface Map {
    setView(center: LatLngTuple | LatLng, zoom: number): this;
    addLayer(layer: Layer): this;
    getZoom(): number;
    latLngToContainerPoint(point: LatLng): Point;
    fitBounds(bounds: LatLngBounds, options: {padding: LatLngTuple}): this;
    invalidateSize(options: {pan: boolean; debounceMoveend: boolean}): this;
  }
  interface Control { addTo(map: Map): this }
  interface ControlOptions { position: string }
  function tileLayer(url: string, options: {attribution: string; maxZoom: number}): TileLayer;
  function markerClusterGroup(): MarkerClusterGroup;
  function marker(point: LatLngTuple, options: {icon: Icon}): Marker;
  function divIcon(options: {className: string; html: string; iconSize: LatLngTuple;
    iconAnchor: LatLngTuple; popupAnchor: LatLngTuple}): Icon;
  const control: {layers(layers: Record<string, TileLayer>): Control};
  const Control: {
    extend<T extends object>(members: T & {options: ControlOptions; onAdd(): HTMLElement}): new() => T & Control;
  };
  const DomUtil: {create(tag: string, className: string): HTMLElement};
  const DomEvent: {
    disableClickPropagation(element: HTMLElement): void;
    disableScrollPropagation(element: HTMLElement): void;
  };
}

interface MapFilterController {
  init(options: {page: string; root: HTMLElement | null; scopeLabel: string; onChange: () => void}): Promise<unknown>;
  getRules?(): unknown[] | {rules?: unknown[]};
  getVisual?(): unknown;
  setResultTotal?(total: number): void;
  setVisualInfo?(info: unknown): void;
}
interface Window {
  VireoFilter?: MapFilterController;
  vireoThumbnailUrl?: (photo: {id: number}) => string;
}
declare var VireoFilter: MapFilterController;

// map-container-size.js
declare function createMapWithContainerResize(containerId: string, options: Record<string, unknown>): L.Map;
// map-selection.js
declare var mapSelectionIds: number[] | null;
declare function mapSelectionScopeLabel(): string | null;
declare function fetchMapSelection(rules: unknown, visual: unknown): Promise<MapPageResponse>;
// map-status.js
declare function mapStatusHtml(data: MapPageResponse): string;
// map-focus.js
declare function hideUnplottableFocusNotice(): void;
declare function showUnplottableFocusNotice(focus: MapPageUnplottableFocus | null | undefined, onShowAll: () => void): void;
// vireo-utils.js and the navbar (navbar-globals.js)
declare function escapeHtml(value: unknown): string;
declare function escapeAttr(value: unknown): string;
declare function safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
