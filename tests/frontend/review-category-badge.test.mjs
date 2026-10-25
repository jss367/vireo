import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {parse} from 'acorn';

// reviewCategoryBadge and its table, lifted out of the classic grid script.
const grid = readFileSync('vireo/static/review/grid.js', 'utf8');
const source = parse(grid, {ecmaVersion: 'latest'}).body
  .filter(node => node.id?.name === 'reviewCategoryBadge'
    || node.declarations?.[0]?.id?.name === 'REVIEW_EFFECTIVE_BADGE_KIND')
  .map(node => grid.slice(node.start, node.end)).join('\n');
const ctx = vm.createContext({});
vm.runInContext(source, ctx);
const badge = pred => {
  const result = ctx.reviewCategoryBadge(pred);
  return result && {cls: result.cls, text: result.text};
};

test('a second subject the fresh comparison calls new is not a disagreement', () => {
  assert.deepEqual(
    badge({category: 'disagreement', effective_category: 'new'}),
    {cls: 'badge-new', text: 'new'},
  );
});

test('a fresh match draws no badge even over a stale disagreement', () => {
  assert.equal(badge({category: 'disagreement', effective_category: 'match'}), null);
});

test('a fresh conflict names only the keywords it was compared with', () => {
  assert.deepEqual(
    badge({category: 'new', effective_category: 'conflict', existing_species: ['Grey Heron']}),
    {cls: 'badge-disagreement', text: 'disagrees with "Grey Heron"'},
  );
});

test('refinement and broader name their keywords', () => {
  assert.equal(
    badge({effective_category: 'refinement', existing_species: ['Egretta']}).text,
    'refines "Egretta"',
  );
  assert.equal(
    badge({effective_category: 'broader', existing_species: ['Little Egret']}).text,
    'broader than "Little Egret"',
  );
});

test('the stored snapshot is used only when there is no fresh comparison', () => {
  assert.deepEqual(
    badge({category: 'disagreement', existing_species: ['Spotted Redshank']}),
    {cls: 'badge-disagreement', text: 'disagrees with "Spotted Redshank"'},
  );
  assert.equal(badge({category: 'match'}), null);
  assert.equal(badge({}), null);
});
