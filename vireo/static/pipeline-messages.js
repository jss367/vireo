// Sorting a finished Process run's messages into failures and notes.
//
// A run's ``result.errors`` lists every message its stages recorded, keyed by
// a ``[stage]`` prefix. Some of them are notes, also listed in
// ``result.notes``: a stage skipped for a reason the user should see (no
// detection qualified for a mask, an optional weight download failed) but
// nothing failed. Labeling those "Failed" tells the user the run broke when
// it didn't, so the Process page shows them as notes. Job rows written before
// notes existed have no ``notes`` key; every entry is then an error, which is
// what those runs meant.
//
// Classic script: declares one page global, loaded before pipeline.html's
// inline script.

// splitPipelineMessages(errors, notes) -> {errors: {stage: msg},
// notes: {stage: msg}, errorList: [msg], noteList: [msg]}. One message per
// stage: a "[stage] Fatal:" entry wins over that stage's other errors, since
// it carries the actionable instruction. A stage with any real error is a
// failure, so its notes are not shown on its card (they stay in noteList).
function splitPipelineMessages(errors, notes) {
  var noteSet = {};
  (Array.isArray(notes) ? notes : []).forEach(function(n) { noteSet[n] = true; });
  var out = {errors: {}, notes: {}, errorList: [], noteList: []};
  (Array.isArray(errors) ? errors : []).forEach(function(msg) {
    var text = String(msg);
    var match = text.match(/^\[(\w+)\]/);
    var stage = match ? match[1] : 'unknown';
    if (noteSet[text]) {
      out.noteList.push(text);
      if (out.notes[stage] === undefined) out.notes[stage] = text;
      return;
    }
    out.errorList.push(text);
    var existing = out.errors[stage];
    var isFatal = text.indexOf('] Fatal:') >= 0;
    if (existing === undefined || (isFatal && existing.indexOf('] Fatal:') < 0)) {
      out.errors[stage] = text;
    }
  });
  for (var stage in out.errors) delete out.notes[stage];
  return out;
}
