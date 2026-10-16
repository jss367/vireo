interface VisiblePollOptions {
  initialDelayMs?: number;
  runWhileHidden?: () => boolean;
}

interface VisiblePollHandle {
  stop(): void;
}

type VisiblePoll = (callback: () => unknown, intervalMs: number, options?: VisiblePollOptions) => VisiblePollHandle;

interface GroupedDecisionTracker {
  alreadyDecided(predictionId: number): boolean;
  record(response: unknown): number[];
  size(): number;
}

interface GroupedPredictions {
  decidedPredictionIds(response: unknown): number[];
  groupedDecisionTracker(): GroupedDecisionTracker;
}
