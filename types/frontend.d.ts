// Contracts for the gradually checked classic-script frontend. These shapes
// describe the selection endpoints and the legacy actions the controllers call.
interface SelectionKeywordSuggestion {
  id: number;
  name: string;
  type?: string;
  count?: number;
  missing_count?: number;
  missing_photo_ids?: number[];
  present_photo_ids?: number[];
}

interface SelectionPredictionSuggestion {
  species: string;
  predicted_count?: number;
  keyworded_count?: number;
  acceptable_photo_count?: number;
  acceptable_keyworded_count?: number;
  acceptable_prediction_ids?: number[];
  predicted_photo_ids?: number[];
  ambiguous_photo_ids?: number[];
  ambiguous_prediction_ids?: number[];
  min_confidence?: number;
  max_confidence?: number;
}

interface SelectionPredictionMeta {
  threshold?: number;
  below_threshold_count?: number;
}

interface KeywordPanelRow {
  readonly name: string;
  readonly missingPhotoIds: readonly number[];
  readonly presentPhotoIds: readonly number[];
}

interface PredictionPanelRow {
  readonly species: string;
  readonly acceptableIds: readonly number[];
  readonly photoIds: readonly number[];
  readonly reviewPhotoId: number | undefined;
}

interface PredictionPanelData {
  predictions: SelectionPredictionSuggestion[];
  selectedCount: number;
  meta: SelectionPredictionMeta;
}

interface PanelRequest {
  isCurrent(): boolean;
  fail(): boolean;
}

interface PanelRequestLane {
  begin(key?: string): PanelRequest | null;
  observe(): () => boolean;
  invalidate(): void;
}

interface BrowseSelectionPanel {
  keywords: {
    reset(): void;
    replace(rows: SelectionKeywordSuggestion[]): void;
    get(id: number): KeywordPanelRow | undefined;
  };
  predictions: {
    reset(): void;
    remember(rows: SelectionPredictionSuggestion[], selectedCount: number, meta: SelectionPredictionMeta): void;
    isExpanded(): boolean;
    toggle(): PredictionPanelData | null;
    replaceRows(rows: SelectionPredictionSuggestion[]): void;
    getRow(index: number): PredictionPanelRow | undefined;
  };
  // Assigned by selection-panel-events.js after the state script loads.
  bindActions?: () => void;
}

interface Window {
  Vireo?: {
    browse?: {
      panelRequests?: Record<'keywords' | 'predictions' | 'detailPredictions' | 'predictionPhotos' | 'wildlife', PanelRequestLane>;
      selectionPanel?: BrowseSelectionPanel;
    };
  };
}

declare function openBatchDevelopmentEditor(): Promise<void>;
declare function pasteEditSettingsToSelection(): Promise<void>;
declare function setSelectionWildlifeExcluded(excluded: boolean): Promise<void>;
declare function applySelectionKeyword(keywordId: number): Promise<void>;
declare function removeSelectionKeyword(keywordId: number): Promise<void>;
declare function toggleSelectionPredictions(): void;
declare function acceptSelectionPrediction(index: number, onAll?: boolean, button?: HTMLButtonElement): Promise<void>;
declare function showSelectionPredictionPhotos(index: number, button?: HTMLButtonElement): Promise<void>;
declare function openPredictionInReview(photoId: number): void;
