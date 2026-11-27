// Vireo.confirm, Vireo.prompt and Vireo.alert (vireo-utils.js): questions and
// notices shown in the page. The native dialogs cannot be used: in the desktop
// app window.confirm() returns a promise instead of the answer, window.alert()
// shows nothing and window.prompt() returns null.

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

interface VireoPromptOptions {
  title: string;
  message?: string;
  // The field's visible label; the title labels the field without one.
  label?: string;
  // The field's starting text, selected so typing replaces it.
  value?: string;
  placeholder?: string;
  // The answer must be one of these: the field is a list instead of a text input.
  choices?: string[];
  confirmLabel?: string;
  cancelLabel?: string;
}

interface VireoAlertOptions {
  title: string;
  message?: string;
  details?: string[];
  // The one button's label; "OK" when omitted.
  confirmLabel?: string;
}

// Resolves true only when the confirm button is pressed.
type VireoConfirm = (options: VireoConfirmOptions) => Promise<boolean>;
// Resolves the field's text, or null when cancelled.
type VireoPrompt = (options: VireoPromptOptions) => Promise<string | null>;
// Resolves when the notice is closed.
type VireoAlert = (options: VireoAlertOptions) => Promise<void>;

interface VireoInPageDialogs {
  confirm: VireoConfirm;
  prompt: VireoPrompt;
  alert: VireoAlert;
}

interface VireoNamespace extends VireoInPageDialogs {}

// What vireo-utils.js builds a dialog from, whichever of the three asked.
type VireoDialogKind = 'confirm' | 'prompt' | 'alert';
type VireoDialogContent = VireoConfirmOptions & VireoPromptOptions & VireoAlertOptions;
interface VireoDialogParts {
  dialog: HTMLDialogElement;
  input: HTMLInputElement | HTMLSelectElement | null;
  cancel: HTMLButtonElement | null;
  accept: HTMLButtonElement;
}
