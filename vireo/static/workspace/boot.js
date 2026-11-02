// Page startup: the module the template loads. Registers the page hook
// vireo-work-locally.js calls, binds the page controls and the delegated list
// listeners, then loads every section.
// ES module. The template loads vireo-work-locally.js (deferred) before this
// module, so its first status load is in flight but cannot have finished yet
// when the hook is registered.

import { byId, fetchActiveWorkspace } from './state.js';
import { registerPageHooks, showCreateWorkspace } from './shared.js';
import { bindFolderList, bindLocalFolderJobProgress, loadWsFolders } from './folders.js';
import { bindMoveFoldersModal } from './move-folders.js';
import { loadHistory } from './history.js';
import { bindWorkspaceList, loadWorkspaces } from './workspaces.js';
import { loadSubjectTypes, saveSubjectTypes } from './subject-types.js';

// vireo-work-locally.js re-renders the folder rows after every local-folder
// status load and job start.
registerPageHooks({ loadWsFolders: loadWsFolders });

byId('saveSubjectTypesBtn').addEventListener('click', function() { saveSubjectTypes(); });
byId('newWorkspaceBtn').addEventListener('click', showCreateWorkspace);
bindFolderList();
bindLocalFolderJobProgress();
bindWorkspaceList();
bindMoveFoldersModal();

/* Set workspace name in page heading */
fetchActiveWorkspace().then(function(ws) {
  if (ws && ws.name) byId('wsPageName').textContent = ws.name;
}).catch(function() {});

loadWsFolders();
loadHistory();
loadWorkspaces();
loadSubjectTypes();
