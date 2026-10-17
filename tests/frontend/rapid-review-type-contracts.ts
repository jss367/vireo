// Compile-only contracts for the Rapid Review classic-script program.
// Invalid calls must remain errors under the page's strict configuration.
decideCurrent('pick');
setZone(1, 'review');
changeQueue({includeRejected: true});
rapid.picks.add(1);

// @ts-expect-error Decisions use queue zones, not persisted flag names.
decideCurrent('flagged');
// @ts-expect-error Photo identifiers are numeric.
rapid.picks.add('1');
// @ts-expect-error Keyword snapshots store boolean membership.
rapid.initialSpeciesKeywords[1] = {mallard: 'yes'};
// @ts-expect-error Queue switches accept boolean controls.
changeQueue({includeRejected: 'yes'});
// @ts-expect-error The pending queue prompt resolves a choice string or null.
rapid.queuePromptResolve = (choice: number) => choice;

const rapidResultsContract: Promise<RapidResults> = safeFetch('/api/pipeline/results');
const rapidStateContract: Promise<RapidGroupResponse> = safeFetch('/api/pipeline/group/state');
// @ts-expect-error Results and keyed group-state photos are different payloads.
const rapidWrongResponseContract: Promise<RapidGroupResponse> = safeFetch('/api/pipeline/results');
void rapidResultsContract;
void rapidStateContract;
void rapidWrongResponseContract;
