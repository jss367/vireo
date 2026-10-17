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

// The server preserves both legacy ID-array bursts and modern object bursts.
const rapidMixedEncounterContract: RapidEncounter = {
  bursts: [[1, 2], {photo_ids: [3], species_override: {species: 'Mallard', confirmed: true}}],
};
buildSessions({encounters: [rapidMixedEncounterContract]});
burstSpeciesList(rapidMixedEncounterContract, [1, 2]);
burstSpeciesList(rapidMixedEncounterContract, {photo_ids: [3]});
locateBurstByPhotoIds([rapidMixedEncounterContract], [1]);
// @ts-expect-error Legacy burst IDs must remain numeric.
const rapidInvalidLegacyBurstContract: RapidBurst = ['1'];
// @ts-expect-error Object burst IDs must remain numeric too.
const rapidInvalidObjectBurstContract: RapidBurst = {photo_ids: ['1']};
// @ts-expect-error A burst override requires a species list, not a scalar.
const rapidInvalidOverrideContract: RapidBurst = {photo_ids: [1], species_override: {species_list: 'Mallard'}};
void rapidInvalidLegacyBurstContract;
void rapidInvalidObjectBurstContract;
void rapidInvalidOverrideContract;
