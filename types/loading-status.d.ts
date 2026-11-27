// VireoLoadingStatus (vireo-utils.js): the line a page shows while its main
// request is running, with elapsed seconds on a long wait and an error state.

interface VireoLoadingStatusHandle {
  // Show `message`. A start during a wait keeps that wait's clock, so a
  // request that restarts itself does not count from zero again. `hint` is a
  // sentence about what is being loaded, shown once the wait is long.
  start(message: string, options?: {hint?: string}): void;
  // The request's results are on screen: hide the line.
  done(): void;
  // The request failed: show `message`, with a button that calls `retry`.
  fail(message: string, options?: {retry?: () => void; retryLabel?: string}): void;
}

interface VireoLoadingStatusApi {
  // `content` is the element the request fills; see vireo-utils.js.
  create(el: HTMLElement, options?: {content?: HTMLElement | null}): VireoLoadingStatusHandle;
}

// What vireo-utils.js keeps for one status line.
interface VireoLoadingStatusView {
  el: HTMLElement;
  content: HTMLElement | null;
  // When the current wait began, in epoch milliseconds.
  startedAt: number;
  ticker: ReturnType<typeof setInterval> | null;
  // Pending while a reload over results has not yet lasted long enough to show.
  quietTimer: ReturnType<typeof setTimeout> | null;
  message: string;
  hint: string;
  messageNode: HTMLElement;
  elapsedNode: HTMLElement;
  hintNode: HTMLElement;
}
