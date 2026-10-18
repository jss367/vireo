interface BrowsePanelLoader<T> {
  load(input: T): Promise<void>;
  retry(): Promise<void>;
}

interface BrowsePanelLoaderOptions<T, R> {
  panel: HTMLElement | null;
  lane: PanelRequestLane;
  accepts(input: T): boolean;
  key(input: T): string | undefined;
  snapshot(input: T): T;
  isActive(input: T): boolean;
  reset(): void;
  request(input: T): Promise<R>;
  render(data: R, input: T): void;
  loadingMessage: string;
  errorMessage: string;
  retryLabel: string;
}

interface SelectionKeywordResponse {
  keywords?: SelectionKeywordSuggestion[];
  selected_count?: number;
}

interface SelectionPredictionResponse extends SelectionPredictionMeta {
  predictions?: SelectionPredictionSuggestion[];
  selected_count?: number;
}

// The detail renderer remains a legacy boundary. Its nested prediction/state
// payload is opaque here: the loader forwards it without inspecting it.
interface DetailPredictionsResponse {
  predictions?: unknown[];
  photo_states?: Record<string, unknown>;
  match_states?: Record<string, unknown>;
}

interface BrowsePanelLoaders {
  keywords: BrowsePanelLoader<readonly number[]>;
  predictions: BrowsePanelLoader<readonly number[]>;
  detailPredictions: BrowsePanelLoader<number>;
}

interface Window {
  VireoPanelLoader: {
    create<T, R>(options: BrowsePanelLoaderOptions<T, R>): BrowsePanelLoader<T>;
  };
}

// The navbar's fetch wrapper (navbar-globals.js), which the shared panel
// loaders and the scripts checked alongside them call.
declare function safeFetch<T>(url: string, options?: RequestInit, errorOptions?: {toast: boolean}): Promise<T>;
