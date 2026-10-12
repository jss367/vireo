import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

const context = vm.createContext({});
vm.runInContext(readFileSync('vireo/static/duplicates-categories.js', 'utf8'), context);
const categories = context.DuplicateCategories;

function group(paths, overrides = {}) {
  const members = paths.map(path => ({path, file_size: 1000}));
  return {winner: members[0], losers: members.slice(1), ...overrides};
}

test('screenshot examples distinguish numbered copies from cross-folder copies', () => {
  assert.equal(categories.classify(group(['/2025/_D850589.NEF', '/2025/_D850589-2.NEF'])),
    'numbered-same-folder');
  assert.equal(categories.classify(group(['/Places/Ant/DSC_1127.NEF', '/Animals/Ant/DSC_1127.NEF'])),
    'same-name-across-folders');
  assert.equal(categories.classify(group(['/Phone/20160627_154208.jpg', '/Tablet/20160627_154208.jpg'])),
    'same-name-across-folders');
});

test('numbered patterns include parentheses, multiple suffixes, and numbered bases', () => {
  for (const names of [
    ['owl.jpg', 'owl (2).jpg'],
    ['owl-2.jpg', 'owl-3.jpg'],
    ['owl-2.jpg', 'owl-2-2.jpg'],
    ['owl.jpg', 'owl-2.jpg', 'owl (3).jpg'],
  ]) {
    assert.equal(categories.classify(group(names.map(name => `/photos/${name}`))), 'numbered-same-folder');
    assert.equal(categories.classify(group(names.map((name, i) => `/photos${i}/${name}`))),
      'numbered-across-folders');
  }
});

test('renamed and mixed groups are distinct from numeric copy suffixes', () => {
  for (const names of [
    ['owl.jpg', 'hawk.jpg'], ['IMG_1234.jpg', 'IMG_1235.jpg'],
    ['owl.jpg', 'owl-2.jpg', 'edited.jpg'], ['owl.jpg', 'owl.NEF'],
    ['owl-2.jpg', 'owl-2.JPG'],
  ]) {
    assert.equal(categories.classify(group(names.map(name => `/photos/${name}`))), 'renamed-same-folder');
    assert.equal(categories.classify(group(names.map((name, i) => `/photos${i}/${name}`))),
      'renamed-across-folders');
  }
});

test('all kept copies contribute to the group pattern, independently of winner choice', () => {
  const proposal = group(['/a/owl.jpg', '/a/owl-2.jpg'], {other_kept: [{path: '/b/renamed.jpg'}]});
  assert.equal(categories.classify(proposal), 'renamed-across-folders');
  const swapped = {...proposal, winner: proposal.losers[0], losers: [proposal.winner]};
  assert.equal(categories.classify(swapped), categories.classify(proposal));
});

test('Windows paths and empty-file groups are handled explicitly', () => {
  assert.equal(categories.classify(group(['C:\\photos\\owl.jpg', 'C:\\photos\\owl-2.jpg'])),
    'numbered-same-folder');
  assert.equal(categories.classify(group(['C:\\photos\\owl.jpg', 'D:\\photos\\owl.jpg'])),
    'same-name-across-folders');
  assert.equal(categories.classify(group(['/a/owl.jpg', '/a/owl-2.jpg'], {empty_file_group: true})),
    'empty-files');
  assert.equal(categories.classify(group(['/owl.jpg', '/owl-2.jpg'])), 'numbered-same-folder');
  assert.equal(categories.classify(group(['/a/owl.jpg', '/a/owl.jpg'])), 'same-path');
  assert.equal(categories.classify({winner: {}, losers: [{}]}), 'other');
});

test('summary counts groups once, counts only extra copies, and sums their sizes', () => {
  const groups = [
    group(['/a/owl.jpg', '/a/owl-2.jpg', '/a/owl-3.jpg']),
    group(['/a/hawk.jpg', '/a/hawk-2.jpg'], {status: 'resolved'}),
    group(['/a/finch.jpg', '/b/finch.jpg'], {other_kept: [{path: '/c/finch.jpg', file_size: 9000}]}),
  ];
  groups[0].losers[1].file_size = 2000;
  assert.deepEqual(JSON.parse(JSON.stringify(categories.summarize(groups))), [
    {id: 'numbered-same-folder', label: 'Numbered copies in one folder', groups: 2, extras: 3, bytes: 4000},
    {id: 'same-name-across-folders', label: 'Same name across folders', groups: 1, extras: 1, bytes: 1000},
  ]);
});
