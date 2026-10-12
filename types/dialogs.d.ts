interface VireoDialogOptions {
  initialFocus?: HTMLElement | null;
  // The owner dismisses through its existing cleanup function, which calls close().
  onDismiss?: () => void;
}

interface VireoDialogController {
  open(returnFocus?: Element | null): void;
  close(): void;
}

interface VireoDialogSession {
  overlay: HTMLElement;
  originalZIndex: string;
  returnFocus: HTMLElement | null;
  focusInside(): void;
  escToken: number;
}

interface Window {
  VireoDialog: {create(overlay: HTMLElement, options?: VireoDialogOptions): VireoDialogController};
  Keymap: {
    pushEsc(handler: () => void): number;
    popEsc(token: number): boolean;
    lockBodyScroll(): void;
    unlockBodyScroll(): void;
  };
}
