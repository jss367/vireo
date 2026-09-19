/* Compile photo search text to the shared filter rule tree. */
(function (root) {
  'use strict';

  function parse(text) {
    if (text.length > 4096) throw new Error('Search is limited to 4,096 characters.');
    const tokens = [];
    let i = 0;
    while (i < text.length) {
      if (/\s/.test(text[i])) { i++; continue; }
      if ('()'.includes(text[i])) { tokens.push({ kind: text[i++] }); continue; }
      if (text[i] === '"') {
        i++;
        let value = '';
        while (i < text.length && text[i] !== '"') {
          // Only quote/backslash escapes are special, preserving file paths.
          if (text[i] === '\\' && ['"', '\\'].includes(text[i + 1])) i++;
          value += text[i++];
        }
        if (i === text.length) throw new Error('Close the quoted phrase with a double quote.');
        i++;
        if (!value.trim()) throw new Error('Quoted phrases cannot be empty.');
        tokens.push({ kind: 'term', value });
      } else {
        const start = i;
        while (i < text.length && !/[\s()"]/.test(text[i])) i++;
        const value = text.slice(start, i);
        tokens.push({ kind: ['AND', 'OR', 'NOT'].includes(value) ? value : 'term', value });
      }
      if (tokens.length > 128) throw new Error('Search is too complex; use fewer words or groups.');
    }
    let pos = 0;
    const peek = () => tokens[pos] && tokens[pos].kind;
    const group = (mode, rules) => rules.length === 1 ? rules[0] : { mode, rules };
    function unary(depth) {
      if (depth > 16) throw new Error('Search parentheses or NOT operators are nested too deeply.');
      if (peek() === 'NOT') {
        pos++;
        return { mode: 'none', rules: [unary(depth + 1)] };
      }
      if (peek() === '(') {
        pos++;
        const node = disjunction(depth + 1);
        if (peek() !== ')') throw new Error('Close the search group with a parenthesis.');
        pos++;
        return node;
      }
      if (peek() !== 'term') throw new Error('Expected a word or quoted phrase.');
      return { field: 'metadata', op: 'contains', value: tokens[pos++].value };
    }
    function conjunction(depth) {
      const rules = [unary(depth)];
      while (peek() && !['OR', ')'].includes(peek())) {
        if (peek() === 'AND') pos++;
        rules.push(unary(depth));
      }
      return group('all', rules);
    }
    function disjunction(depth) {
      const rules = [conjunction(depth)];
      while (peek() === 'OR') {
        pos++;
        rules.push(conjunction(depth));
      }
      return group('any', rules);
    }
    if (!tokens.length) return { mode: 'all', rules: [] };
    const result = disjunction(0);
    if (pos !== tokens.length) throw new Error('Unexpected closing parenthesis.');
    return result;
  }

  root.VireoSearch = { parse };
  if (typeof module !== 'undefined' && module.exports) module.exports = { parse };
})(typeof window !== 'undefined' ? window : globalThis);
