// Load the complete owner implementations into isolated VM-based UI fixtures.
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
function installBrowseOwners(context) {
  for (const [file, name] of [['selection-state', 'selection'], ['pagination', 'pagination']]) {
    const source = fs.readFileSync(path.join(__dirname, '../../vireo/static/browse', file + '.js'), 'utf8');
    context[name] = vm.runInContext('(function() {\n' + source.replace(/^export /gm, '') + '\nreturn ' + name + ';\n})()', context);
  }
}
module.exports = {installBrowseOwners};
