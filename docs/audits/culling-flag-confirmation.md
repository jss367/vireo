# Culling flag confirmation

A catalog flag alone does not prove that the photographer picked a photo:
ranking jobs and older integrations also write flags. Cull now uses a current,
matching manual-attribution record before showing a saved keep or reject as
confirmed. Missing historical attribution stays unknown. Existing raw flags are
not cleared or retrospectively relabeled as manual decisions.

Fresh analysis and cached results read this evidence from the catalog. Pipeline
page/API/reflow/regroup payloads overwrite cached confirmation claims with live
state, and the Cull client requires that state. Explicit Apply records keep,
reject and Review decisions atomically with flags, sidecar intents and one undo
entry. Undo and redo restore captured attribution; old undo entries without a
snapshot cannot manufacture it. Schema migration 28 adds these replay snapshots.
History pruning removes snapshots without deleting current attribution.

Sharpness ranking can flag an unchosen candidate, but cannot replace an existing
flag or an explicitly confirmed clear flag. Its provisional flag remains
unconfirmed in Cull. This correction addresses flag origin and persistence; it
does not claim that ranking alone supplies missing absolute quality evidence.

Local validation uses synthetic catalogs and images, socket-free Flask clients,
and Node VM tests of the actual Cull scripts. Full pytest was not executed:
pytest-rerunfailures attempted a prohibited localhost socket bind during startup,
before collection. No plugin/environment workaround was used. Required CI and
latest-head review remain merge gates; direct tests are not a full-suite pass.
