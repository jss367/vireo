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
}
