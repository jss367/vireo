// Run one ES module's body in a node:vm context, the way the classic-script
// tests did before the navbar became modules: its `import` declarations are
// dropped (the test supplies those names as context globals) and `export`
// keywords removed, so its top-level declarations become context properties.

/** @param {string} source @returns {string} */
export function moduleAsScript(source) {
  return source
    .replace(/^import\s[^;]*?\sfrom\s+'[^']+';\n/gm, '')
    .replace(/^import\s+'[^']+';\n/gm, '')
    .replace(/(^|\*\/ )export (?=(?:async\s+)?function|const |let |var |class )/gm, '$1');
}
