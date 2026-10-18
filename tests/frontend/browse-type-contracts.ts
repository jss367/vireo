// Browse's panel controllers are ES module exports; these expected failures
// keep checkJs from silently turning their public API into `any`. An unused
// expect-error directive fails the check.
import { panelLoaders } from '../../vireo/static/browse/panel-loaders.js';
import { panelRequests } from '../../vireo/static/browse/panel-requests.js';
import {
  acceptSelectionPrediction, showSelectionPredictionPhotos,
} from '../../vireo/static/browse/prediction-panels.js';
import { selectionPanel } from '../../vireo/static/browse/selection-panel-state.js';

export function verifyBrowseContracts() {
  const panel = selectionPanel;
  const lane = panelRequests.keywords;
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

  panelLoaders.keywords.load([1, 2]);
  panelLoaders.detailPredictions.load(1);
  panelLoaders.predictions.retry();
  // @ts-expect-error Photo ids cannot be strings.
  panelLoaders.keywords.load(['1', '2']);
  // @ts-expect-error The detail panel loads a single photo id.
  panelLoaders.detailPredictions.load([1, 2]);
  // @ts-expect-error Retry uses its owned snapshot, not caller-supplied ids.
  panelLoaders.predictions.retry([3, 4]);
}
