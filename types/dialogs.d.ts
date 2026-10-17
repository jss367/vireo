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

interface VireoKeymap {
  pushEsc(handler: () => void): number;
  popEsc(token: number): boolean;
  lockBodyScroll(): void;
  unlockBodyScroll(): void;
}

interface Window {
  VireoDialog: {
    create(overlay: HTMLElement, options?: VireoDialogOptions): VireoDialogController;
    // True while any VireoDialog session is on the stack. Owners of other
    // overlays (help, report issue) use it to no-op their keyboard
    // shortcuts: updateBackground() marks their panel inert while a dialog
    // is open, so showing them in that state would be a dead overlay.
    hasActive(): boolean;
  };
  Keymap: VireoKeymap;
}
