/* Filename/folder patterns within already verified exact-duplicate groups. */
export const DuplicateCategories = (function() {
  var definitions = [
    {id: 'numbered-same-folder', label: 'Numbered copies in one folder'},
    {id: 'same-name-across-folders', label: 'Same name across folders'},
    {id: 'numbered-across-folders', label: 'Numbered copies across folders'},
    {id: 'renamed-same-folder', label: 'Different names in one folder'},
    {id: 'renamed-across-folders', label: 'Different names across folders'},
    {id: 'same-path', label: 'Repeated catalog path'},
    {id: 'empty-files', label: 'Zero-byte files'},
    {id: 'other', label: 'Other patterns'},
  ];

  /** @param {DuplicateMember} photo */
  function fileParts(photo) {
    var path = (photo.path || '').replace(/\\/g, '/');
    var split = path.lastIndexOf('/');
    var name = photo.filename || path.substring(split + 1);
    var dot = name.lastIndexOf('.');
    return {
      folder: path.substring(0, split + 1),
      name: name,
      stem: dot > 0 ? name.substring(0, dot) : name,
      extension: dot > 0 ? name.substring(dot).toLowerCase() : '',
    };
  }

  // Only recognize trailing copy-style numbers, never embedded camera
  // sequence numbers (IMG_1234) or arbitrary differences in the filename.
  /** @param {string} stem */
  function stripNumber(stem) {
    return stem.replace(/(?:-[1-9]\d*| \([1-9]\d*\))$/, '');
  }

  /** @param {DuplicateCategoryGroup} group */
  function classify(group) {
    if (group.empty_file_group) return 'empty-files';
    var members = [group.winner].concat(group.other_kept || [], group.losers || [])
      .filter(Boolean).map(fileParts);
    if (members.length < 2 || members.some(function(p) { return !p.name || !p.folder; })) {
      return 'other';
    }
    const first = members[0];
    if (!first) return 'other';
    var sameFolder = members.every(function(p) { return p.folder === first.folder; });
    if (members.every(function(p) { return p.name === first.name; })) {
      return sameFolder ? 'same-path' : 'same-name-across-folders';
    }
    // A real base filename takes precedence, including bases that themselves
    // end in a number (owl-2.jpg alongside owl-2-2.jpg).
    var base = members.reduce(function(a, b) { return a.stem.length <= b.stem.length ? a : b; });
    var numbered = members.every(function(p) {
      return p.extension === base.extension &&
        (p.stem === base.stem || stripNumber(p.stem) === base.stem);
    });
    if (!numbered) {
      var root = stripNumber(first.stem);
      numbered = members.every(function(p) {
        return p.extension === first.extension && stripNumber(p.stem) === root;
      }) && members.some(function(p) { return stripNumber(p.stem) !== p.stem; });
    }
    if (numbered && members.some(function(p) { return p.stem !== first.stem; })) {
      return sameFolder ? 'numbered-same-folder' : 'numbered-across-folders';
    }
    return sameFolder ? 'renamed-same-folder' : 'renamed-across-folders';
  }

  /** @param {string} id */
  function label(id) {
    var definition = definitions.find(function(d) { return d.id === id; });
    return definition ? definition.label : 'Other patterns';
  }

  /** @param {DuplicateCategoryGroup[]} groups */
  function summarize(groups) {
    /** @type {Record<string, DuplicateCategorySummary>} */
    var counts = {};
    groups.forEach(function(group) {
      var id = classify(group);
      var count = counts[id] || (counts[id] = {id: id, label: label(id), groups: 0, extras: 0, bytes: 0});
      count.groups++;
      (group.losers || []).forEach(function(p) {
        count.extras++;
        count.bytes += p.file_size || 0;
      });
    });
    return definitions.map(function(d) { return counts[d.id]; }).filter(function(row) { return row !== undefined; });
  }

  return {classify: classify, label: label, summarize: summarize};
})();
