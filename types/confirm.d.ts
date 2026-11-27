// Vireo.confirm (vireo-utils.js): a yes/no question asked in the page.
// window.confirm() cannot gate an action in the desktop app, where it
// returns a promise instead of the answer.
interface VireoConfirmOptions {
  title: string;
  // Paragraphs are separated by a blank line.
  message?: string;
  // One list row each, for example the files the action removes.
  details?: string[];
  confirmLabel?: string;
  cancelLabel?: string;
  // Style the confirm button as destructive.
  danger?: boolean;
}

// Resolves true only when the confirm button is pressed.
type VireoConfirm = (options: VireoConfirmOptions) => Promise<boolean>;

interface VireoNamespace {
  confirm: VireoConfirm;
}
