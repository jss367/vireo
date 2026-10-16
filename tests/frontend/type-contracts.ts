// These expected failures ensure checkJs does not silently turn the checked
// public API into `any`. Unused @ts-expect-error directives fail the check.
function verifyFrontendContracts() {
  const panel = window.Vireo?.browse?.selectionPanel;
  const lane = window.Vireo?.browse?.panelRequests?.keywords;
  if (!panel || !lane) return;
  panel.keywords.replace([{id: 7, name: "Cooper's Hawk", missing_photo_ids: [1, 2]}]);
  panel.predictions.replaceRows([{species: "Say's Phoebe", acceptable_prediction_ids: [100]}]);
  lane.begin('1,2')?.isCurrent();

  // @ts-expect-error Selection photo ids must be numbers.
  panel.keywords.replace([{id: 7, name: 'Hawk', missing_photo_ids: ['1']}]);
  // @ts-expect-error The species field is required in prediction suggestions.
  panel.predictions.replaceRows([{acceptable_prediction_ids: [100]}]);
  // @ts-expect-error Panel rows are immutable snapshots.
  panel.predictions.getRow(0)?.acceptableIds.push(101);
  // @ts-expect-error Selection cache keys are strings.
  lane.begin(12);
  // @ts-expect-error Delegated acceptance must use a numeric row index.
  acceptSelectionPrediction('0', true);
  // @ts-expect-error A button argument must be a DOM button.
  showSelectionPredictionPhotos(0, {});

  const dialog = window.VireoDialog.create(document.createElement('div'), {
    initialFocus: document.createElement('input'),
  });
  dialog.open(document.createElement('button'));
  dialog.close();
  // @ts-expect-error A dialog must have an HTML element as its container.
  window.VireoDialog.create(document);
  // @ts-expect-error Dismissal cleanup is a callback.
  window.VireoDialog.create(document.createElement('div'), {onDismiss: 'close'});
  // @ts-expect-error Focus returns to an element, not a selector string.
  dialog.open('#opener');

  const loaders = window.Vireo?.browse?.panelLoaders;
  if (loaders) {
    loaders.keywords.load([1, 2]);
    loaders.detailPredictions.load(1);
    loaders.predictions.retry();
    // @ts-expect-error Photo ids cannot be strings.
    loaders.keywords.load(['1', '2']);
    // @ts-expect-error The detail panel loads a single photo id.
    loaders.detailPredictions.load([1, 2]);
    // @ts-expect-error Retry uses its owned snapshot, not caller-supplied ids.
    loaders.predictions.retry([3, 4]);
  }

  // Navbar globals (types/navbar.d.ts) and the small shared classic scripts.
  showToast('Exported 2 photos', 'success');
  safeEventSource('/api/jobs/1/stream', {quietError: true, onError: () => {}});
  // @ts-expect-error A toast message is a string.
  showToast(42);
  // @ts-expect-error quietError is a flag, not a message.
  safeEventSource('/api/jobs/1/stream', {quietError: 'yes'});

  VireoExportJob.outcome({status: 'completed', result: {exported: 2, errors: []}}).message;
  // @ts-expect-error The exported count is a number.
  VireoExportJob.outcome({status: 'completed', result: {exported: '2'}});
  // @ts-expect-error A job id is a string.
  VireoExportJob.watch(7);

  splitPipelineMessages(['[classify] Fatal: no model'], null).errorList.length;
  // @ts-expect-error Split messages are strings keyed by stage, not objects.
  splitPipelineMessages([], []).errors.classify?.stage;

  describeUnplottableFocus({reason: 'not_found'}).actions.length;
  // @ts-expect-error Location keywords carry numeric ids.
  describeUnplottableFocus({location_keywords: [{id: '7', name: 'Park'}]});
  // @ts-expect-error Show-all is a callback.
  showUnplottableFocusNotice({reason: 'unavailable'}, 'reload');

  canRetryImportDirectToNas({id: 'j1', type: 'import', status: 'failed'}, []);
  // @ts-expect-error A job row needs an id.
  canRetryImportDirectToNas({type: 'import', status: 'failed'}, []);
  // @ts-expect-error The retry body is an object, not a JSON string.
  directNasImportRetryBody({id: 'j1'}, '{}', []);
}
