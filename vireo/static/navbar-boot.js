// The navbar's entry module, the only navbar script _navbar.html loads:
//
//   <script type="module" src="/static/navbar-boot.js"></script>
//
// It imports the navbar and lightbox modules in dependency order (a module's
// own imports still evaluate before it) and the classic bridge
// (navbar-globals.js, which imports every
// module it publishes from), then binds the markup's data-navbar-on handlers.
// Module scripts are deferred: this runs after the document is parsed, before
// the page's deferred scripts and before DOMContentLoaded.

import './navbar-state.js';
import './navbar-command-palette-open.js';
import './navbar-edit-nav.js';
import './navbar-report-context-menu.js';
import './navbar-tabs.js';
import './navbar-tab-reorder-hotkeys.js';
import './navbar-workspace-switcher-init.js';
import './lightbox/shared-state.js';
import './lightbox/session.js';
import './lightbox/viewport.js';
import './lightbox/state.js';
import './lightbox/curation.js';
import './lightbox/edits.js';
import './lightbox/flags.js';
import './lightbox/overlays.js';
import './lightbox/crop.js';
import './lightbox/navigation.js';
import './lightbox/controls.js';
import './lightbox/delete.js';
import './lightbox/inat.js';
import './lightbox/keyboard.js';
import './lightbox/source-loading.js';
import './navbar-pipeline-inspector.js';
import './navbar-similar-photos.js';
import './navbar-shared-helpers.js';
import './navbar-folder-banners.js';
import './navbar-command-palette.js';
import './navbar-lightbox-subjects.js';
import { bindCommandPaletteKeys } from './navbar-command-palette-open.js';
import { bindShortcutsSheetKeys } from './navbar-tab-reorder-hotkeys.js';
import { bindLightboxKeyboard } from './lightbox/keyboard.js';
import { bindNavbarMarkup } from './navbar-markup.js';
import './navbar-globals.js';

// The markup's handlers were inline attributes, attached before any script
// listener, so bind them first.
bindNavbarMarkup();
// Modules evaluate in dependency order, not the order the classic scripts
// loaded in. Listeners that share an event with another module's, where one
// may stop the event reaching the next, are attached here in the classic order.
bindCommandPaletteKeys();
bindShortcutsSheetKeys();
bindLightboxKeyboard();
