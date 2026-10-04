// Unit tests for pure helpers in the shared navbar scripts (vireo/static/navbar-*.js).
// Each helper is lifted out of the file the browser actually loads and run in
// an isolated context, so a change to the shipped code is what gets tested.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const STATIC = 'vireo/static/';
function source(file) {
  return [].concat(file).map(f => fs.readFileSync(STATIC + f, 'utf8')).join('\n');
}
function fn(file, name) {
  const found = source(file).match(new RegExp('(?:async )?function ' + name + '\\([^]*?\\n\\}'));
  assert(found, file + ': function ' + name);
  return found[0];
}
function decl(file, name) {
  const found = source(file).match(new RegExp('(?:var|let|const) ' + name + ' = [^]*?;\\n'));
  assert(found, file + ': declaration ' + name);
  return found[0];
}
function load(parts, globals) {
  const ctx = vm.createContext(Object.assign({}, globals));
  vm.runInContext(parts.join('\n'), ctx);
  return ctx;
}

const tests = [];
function test(name, body) { tests.push([name, body]); }

const inspector = 'navbar-pipeline-inspector.js';
const banners = 'navbar-folder-banners.js';
const helpers = 'navbar-shared-helpers.js';
// Follow the same order as the browser instead of duplicating the script list.
const lightbox = Array.from(
  fs.readFileSync('vireo/templates/_navbar.html', 'utf8').matchAll(
    /<script src="\/static\/(lightbox\/[^"]+\.js)"><\/script>/g
  ), match => match[1]
);

test('safeEventSource toasts a dropped stream unless the caller shows its own message', () => {
  for (const [quietError, expected] of [[undefined, ['Connection lost']], [true, []]]) {
    const toasts = [];
    let source;
    let handled = 0;
    const ctx = load([fn(helpers, 'safeEventSource')], {
      EventSource: function(url) {
        source = this;
        this.addEventListener = () => {};
        this.close = () => { this.closed = true; };
      },
      showToast: (message) => toasts.push(message),
    });
    ctx.safeEventSource('/api/jobs/x/stream', {quietError, onError: () => { handled += 1; }});
    source.onerror();
    assert.equal(source.closed, true);
    assert.equal(handled, 1);
    assert.deepEqual(toasts, expected);
  }
});

test('formatMatchScore renders logits and probabilities at their own precision', () => {
  const ctx = load([fn(inspector, 'formatMatchScore')]);
  assert.equal(ctx.formatMatchScore(null, 'logit'), '—');
  assert.equal(ctx.formatMatchScore(undefined), '—');
  assert.equal(ctx.formatMatchScore(12.345, 'logit'), '12.3');
  assert.equal(ctx.formatMatchScore(0.12345, 'probability'), '0.123');
  assert.equal(ctx.formatMatchScore('0.5'), '0.500');
  assert.equal(ctx.formatMatchScore(0, 'logit'), '0.0');
});

test('matchRunVerdict names superseded runs and never invents a verdict', () => {
  const ctx = load([decl(inspector, 'MATCH_RUN_VERDICTS'), fn(inspector, 'matchRunVerdict')]);
  const verdict = row => ctx.matchRunVerdict(row).label;
  assert.equal(verdict({assessment: {state: 'listed'}}), 'match');
  assert.equal(verdict({assessment: {state: 'unlisted'}}), 'no match');
  assert.equal(verdict({assessment: {state: 'uncalibrated'}}), 'not judged');
  // A superseded run is history, whatever it once scored.
  assert.equal(verdict({is_current: 0, assessment: {state: 'listed'}}), 'superseded');
  assert.equal(verdict({is_current: false, assessment: {state: 'listed'}}), 'superseded');
  assert.equal(verdict({is_current: 1, assessment: {state: 'listed'}}), 'match');
  // Missing or unknown states read as "not recorded", not as a pass.
  assert.equal(verdict({}), 'not recorded');
  assert.equal(verdict({assessment: {state: 'something-new'}}), 'not recorded');
});

test('_parseFolderHealthVersion accepts only non-negative safe integers', () => {
  const ctx = load([fn(banners, '_parseFolderHealthVersion')]);
  assert.equal(ctx._parseFolderHealthVersion('7'), 7);
  assert.equal(ctx._parseFolderHealthVersion(0), 0);
  assert.equal(ctx._parseFolderHealthVersion('-1'), null);
  assert.equal(ctx._parseFolderHealthVersion('1.5'), null);
  assert.equal(ctx._parseFolderHealthVersion('abc'), null);
  assert.equal(ctx._parseFolderHealthVersion(2 ** 53), null);
});

test('_missingFolderIdsDiffer compares ordered id snapshots', () => {
  const ctx = load([fn(banners, '_missingFolderIdsDiffer')]);
  assert.equal(ctx._missingFolderIdsDiffer([], []), false);
  assert.equal(ctx._missingFolderIdsDiffer([1, 2], [1, 2]), false);
  assert.equal(ctx._missingFolderIdsDiffer([1, 2], [2, 1]), true);
  assert.equal(ctx._missingFolderIdsDiffer([1], [1, 2]), true);
  assert.equal(ctx._missingFolderIdsDiffer([1, 2], [1]), true);
});

test('_offlineRootsPhrase factors out the shared parent and caps the list', () => {
  const ctx = load([
    decl(banners, 'OFFLINE_ROOTS_MAX_LISTED'),
    fn(banners, '_commonParentDepth'),
    fn(banners, '_offlineRootsPhrase'),
  ]);
  const phrase = roots => ctx._offlineRootsPhrase(roots);
  assert.equal(phrase(['/Volumes/NAS/Photos']), '/Volumes/NAS/Photos is');
  assert.equal(
    phrase(['/Volumes/NAS/Photos/2024', '/Volumes/NAS/Photos/2023']),
    '2 folders in /Volumes/NAS/Photos (2023, 2024) are',
  );
  // Only the filesystem root in common: nothing worth naming as a parent.
  assert.equal(phrase(['/b/y', '/a/x']), '2 folders (/a/x, /b/y) are');
  // Windows paths keep their own separator.
  assert.equal(
    phrase(['D:\\Photos\\2024', 'D:\\Photos\\2023']),
    '2 folders in D:\\Photos (2023, 2024) are',
  );
  const eight = [8, 7, 6, 5, 4, 3, 2, 1].map(n => '/v/r/f' + n);
  assert.equal(phrase(eight), '8 folders in /v/r (f1, f2, f3, f4, f5, f6 and 2 more) are');
  // The caller's array is not reordered.
  assert.equal(eight[0], '/v/r/f8');
});

test('_offlineRootsKey is order-independent and does not mutate its input', () => {
  const ctx = load([fn(banners, '_offlineRootsKey')]);
  const roots = ['/b', '/a'];
  assert.equal(ctx._offlineRootsKey(roots), '/a\n/b');
  assert.equal(ctx._offlineRootsKey(['/a', '/b']), ctx._offlineRootsKey(roots));
  assert.deepEqual(roots, ['/b', '/a']);
});

test('formatBytesNav picks the unit at each 1024 boundary', () => {
  const ctx = load([fn(banners, 'formatBytesNav')]);
  assert.equal(ctx.formatBytesNav(null), '');
  assert.equal(ctx.formatBytesNav(undefined), '');
  assert.equal(ctx.formatBytesNav(0), '0 B');
  assert.equal(ctx.formatBytesNav(1023), '1023 B');
  assert.equal(ctx.formatBytesNav(1024), '1.0 KB');
  assert.equal(ctx.formatBytesNav(1536), '1.5 KB');
  assert.equal(ctx.formatBytesNav(1024 * 1024), '1.0 MB');
  assert.equal(ctx.formatBytesNav(5 * 1024 ** 3), '5.00 GB');
});

test('job predicates agree on which jobs are live, waiting and badge-worthy', () => {
  const ctx = load(['isLiveJob', 'countsForBadge', 'isAttentionJob', 'isWaitingJob']
    .map(name => fn(helpers, name)));
  for (const status of ['running', 'pausing', 'paused', 'queued', 'pending']) {
    assert.equal(ctx.isLiveJob({status}), true, status);
  }
  for (const status of ['completed', 'failed', 'cancelled', undefined]) {
    assert.equal(ctx.isLiveJob({status}), false, String(status));
  }
  assert.equal(ctx.isLiveJob(null), false);

  assert.equal(ctx.countsForBadge({}), true);
  assert.equal(ctx.countsForBadge({counts_for_badge: false}), false);
  assert.equal(ctx.isAttentionJob({status: 'running'}), true);
  assert.equal(ctx.isAttentionJob({status: 'running', counts_for_badge: false}), false);
  assert.equal(ctx.isAttentionJob({status: 'completed'}), false);

  assert.equal(ctx.isWaitingJob({status: 'queued'}), true);
  assert.equal(ctx.isWaitingJob({status: 'pending'}), true);
  assert.equal(ctx.isWaitingJob({status: 'running'}), false);
  assert.equal(ctx.isWaitingJob(null), false);
});

test('revealFeedbackMessage reports failures and names the file manager', () => {
  const ctx = load([fn(helpers, 'revealFeedbackMessage')], {window: {}});
  assert.equal(ctx.revealFeedbackMessage({ok: false, reason: 'not found'}), 'Reveal failed: not found');
  assert.equal(ctx.revealFeedbackMessage({ok: false}), 'Reveal failed: unknown error');
  assert.equal(ctx.revealFeedbackMessage({ok: true}), 'Revealed in file manager');
  ctx.window.VIREO_FILE_MANAGER_NAME = 'Finder';
  assert.equal(ctx.revealFeedbackMessage(null), 'Revealed in Finder');
});

test('_vireoUrlWithQueryParam appends before the fragment and encodes', () => {
  const ctx = load([fn(lightbox, '_vireoUrlWithQueryParam')]);
  const add = (...args) => ctx._vireoUrlWithQueryParam(...args);
  assert.equal(add('/photos/1/full', 'rv', 'abc'), '/photos/1/full?rv=abc');
  assert.equal(add('/x?a=1#frag', 'rv', 'abc'), '/x?a=1&rv=abc#frag');
  assert.equal(add('/x', 'a b', 'c&d'), '/x?a%20b=c%26d');
  assert.equal(add('/x', 'rv', ''), '/x');
  assert.equal(add('/x', 'rv', null), '/x');
  assert.equal(add('', 'rv', 'abc'), '');
});

test('_vireoUrlWithPhotoSource replaces rather than duplicates the source param', () => {
  const ctx = load([fn(lightbox, '_vireoUrlWithQueryParam'), fn(lightbox, '_vireoUrlWithPhotoSource')]);
  const withSource = (...args) => ctx._vireoUrlWithPhotoSource(...args);
  assert.equal(withSource('/p/1/full', 'raw'), '/p/1/full?source=raw');
  assert.equal(withSource('/p/1/full?source=raw&v=2', 'jpeg'), '/p/1/full?v=2&source=jpeg');
  assert.equal(withSource('/p/1/full?v=2&source=raw', 'raw'), '/p/1/full?v=2&source=raw');
  assert.equal(withSource('/p/1/full?source=raw', ''), '/p/1/full');
});

test('_vireoCleanRenderSearch drops only the cache-busting params', () => {
  const ctx = load([fn(lightbox, '_vireoCleanRenderSearch')], {URLSearchParams});
  assert.equal(ctx._vireoCleanRenderSearch(''), '');
  assert.equal(ctx._vireoCleanRenderSearch('?rv=1'), '');
  assert.equal(ctx._vireoCleanRenderSearch('?rv=1&er=2&editv=3&v=4&source=raw'), '?source=raw');
  assert.equal(ctx._vireoCleanRenderSearch('?size=1024&v=9'), '?size=1024');
});

test('_vireoUrlWithRenderVersion applies only a known per-photo version', () => {
  const ctx = load(
    [fn(lightbox, '_vireoUrlWithQueryParam'), fn(lightbox, '_vireoUrlWithRenderVersion')],
    {_lbRenderVersionByPhoto: {'5': 'v1'}},
  );
  assert.equal(ctx._vireoUrlWithRenderVersion('/p/5/full', 5), '/p/5/full?rv=v1');
  assert.equal(ctx._vireoUrlWithRenderVersion('/p/5/full', '5'), '/p/5/full?rv=v1');
  assert.equal(ctx._vireoUrlWithRenderVersion('/p/6/full', 6), '/p/6/full');
  assert.equal(ctx._vireoUrlWithRenderVersion('/p/5/full', null), '/p/5/full');
});

test('a partly failed delete job keeps its retained photos and reports both counts', () => {
  const calls = {toasts: [], confirms: [], callback: null, hidden: 0};
  const ctx = load([fn(lightbox, 'handleDeleteJobComplete')], {
    hideDeleteModal: () => { calls.hidden++; },
    showToast: (msg, kind) => { calls.toasts.push([msg, kind]); },
    confirm: msg => { calls.confirms.push(msg); return false; },
    safeFetch: () => { throw new Error('no retry expected'); },
  });
  ctx.handleDeleteJobComplete({
    status: 'failed',
    errors: ['/a/2.jpg: SMB Trash unavailable'],
    result: {deleted: 1, trashed: 1, failed_photo_ids: [2],
             trash_failed: [{path: '/a/2.jpg', error: 'SMB Trash unavailable', photo_id: 2}]},
  }, data => { calls.callback = data; }, 'disk', false);
  // The permanent-delete fallback is still offered for the retained file...
  assert.equal(calls.confirms.length, 1);
  // ...and the grid callback still runs, so the deleted photo leaves the grid.
  assert.deepEqual(Array.from(calls.callback.failed_photo_ids), [2]);
  assert.deepEqual(calls.toasts, [['1 photo moved to Trash; 1 retained after file errors', 'error']]);
});

test('a delete job that failed outright reports the failure and runs no callback', () => {
  const calls = {toasts: [], callback: null};
  const ctx = load([fn(lightbox, 'handleDeleteJobComplete')], {
    hideDeleteModal: () => {},
    showToast: (msg, kind) => { calls.toasts.push([msg, kind]); },
    confirm: () => { throw new Error('no prompt expected'); },
  });
  ctx.handleDeleteJobComplete(
    {status: 'failed', errors: ['database is locked'], result: null},
    data => { calls.callback = data; }, 'disk', false,
  );
  assert.equal(calls.callback, null);
  assert.deepEqual(calls.toasts, [['Delete failed: database is locked', 'error']]);
});

// A fake clock and document for vireo-visible-poll.js: timers fire only when
// the test advances time, and visibility flips dispatch visibilitychange.
function visiblePollHarness() {
  const env = {now: 0, timers: [], nextId: 1, listeners: [], hidden: false};
  env.document = {
    get hidden() { return env.hidden; },
    addEventListener(type, fn) { if (type === 'visibilitychange') env.listeners.push(fn); },
    removeEventListener(type, fn) { env.listeners = env.listeners.filter(l => l !== fn); },
  };
  const ctx = load([source('vireo-visible-poll.js')], {
    window: {document: env.document},
    Date: {now: () => env.now},
    setTimeout(fn, ms) { const id = env.nextId++; env.timers.push({id, at: env.now + ms, fn}); return id; },
    clearTimeout(id) { env.timers = env.timers.filter(t => t.id !== id); },
  });
  env.poll = (...args) => ctx.window.Vireo.pollWhileVisible(...args);
  env.advance = (ms) => {
    const end = env.now + ms;
    for (;;) {
      env.timers.sort((a, b) => a.at - b.at);
      const next = env.timers[0];
      if (!next || next.at > end) break;
      env.timers.shift();
      env.now = next.at;
      next.fn();
    }
    env.now = end;
  };
  env.setHidden = (hidden) => { env.hidden = hidden; env.listeners.slice().forEach(l => l()); };
  return env;
}

test('pollWhileVisible ticks on its interval while visible', () => {
  const env = visiblePollHarness();
  const runs = [];
  env.poll(() => runs.push(env.now), 1000);
  env.advance(3500);
  assert.deepEqual(runs, [1000, 2000, 3000]);
});

test('pollWhileVisible schedules nothing while hidden and catches up on return', () => {
  const env = visiblePollHarness();
  const runs = [];
  env.poll(() => runs.push(env.now), 1000);
  env.advance(1000);
  env.setHidden(true);
  env.advance(60000);
  assert.deepEqual(runs, [1000]);
  assert.equal(env.timers.length, 0);
  // The overdue tick runs at once, then the normal cadence resumes.
  env.setHidden(false);
  env.advance(0);
  assert.deepEqual(runs, [1000, 61000]);
  env.advance(1000);
  assert.deepEqual(runs, [1000, 61000, 62000]);
});

test('pollWhileVisible keeps an undue tick on schedule across a brief hide', () => {
  const env = visiblePollHarness();
  const runs = [];
  env.poll(() => runs.push(env.now), 1000, {initialDelayMs: 5000});
  env.advance(1000);
  env.setHidden(true);
  env.setHidden(false);
  env.advance(3999);
  assert.deepEqual(runs, []);
  env.advance(1);
  assert.deepEqual(runs, [5000]);
});

test('pollWhileVisible runWhileHidden keeps ticking while its predicate holds', () => {
  const env = visiblePollHarness();
  const runs = [];
  let busy = true;
  env.poll(() => runs.push(env.now), 1000, {runWhileHidden: () => busy});
  env.setHidden(true);
  env.advance(2000);
  busy = false;
  env.advance(5000);
  assert.deepEqual(runs, [1000, 2000]);
});

test('pollWhileVisible stop cancels the poll and its visibility listener', () => {
  const env = visiblePollHarness();
  const runs = [];
  const handle = env.poll(() => runs.push(env.now), 1000);
  env.setHidden(true);
  env.advance(2000);
  handle.stop();
  env.setHidden(false);
  env.advance(5000);
  assert.deepEqual(runs, []);
  assert.equal(env.listeners.length, 0);
});


test('checkNewImages shows zero-count local-copy exclusions and allows dismissal', async () => {
  const msg = {textContent: '', title: ''};
  const cta = {style: {}};
  const banner = {dataset: {}, style: {}, querySelector: () => cta};
  const removed = [];
  let dismissed = false;
  let localOnly = true;
  const ctx = load([
    fn(banners, 'checkNewImages'),
    fn(banners, '_localCopiesSentence'),
    fn(banners, '_appendBannerTitlePaths'),
  ], {
    _newImagesInFlight: false, _newImagesForcedRerun: false,
    _newImagesInvalidatedToken: 0, _newImagesPendingTimer: null,
    _newImagesRecheckToken: 0,
    document: {getElementById: id => id === 'newImagesBanner' ? banner : msg},
    fetch: async () => ({ok: true, json: async () => ({
      workspace_id: 1, new_count: 0,
      unreachable_roots: localOnly ? [] : ['/nas/photos'],
      local_copy_excluded: localOnly ? ['/nas/photos'] : [],
    })}),
    sessionStorage: {removeItem: key => removed.push(key)},
    _newImagesDismissKey: () => 'count', _newImagesOfflineDismissKey: () => 'roots',
    _offlineRootsKey: roots => roots.join('\n'),
    _offlineRootsPhrase: roots => roots[0] + ' is',
    _isNewImagesDismissed: (_ws, count, roots) => {
      assert.equal(count, 0);
      assert.equal(roots[0], (localOnly ? 'local:' : 'offline:') + '/nas/photos');
      return dismissed && roots[0] === 'local:/nas/photos';
    },
    _applyOfflineBannerDetail: () => {msg.title = '';},
    _newImagesAnswersRecheck: () => true, _setNewImagesRecheckBusy: () => {},
    _failNewImagesRecheck: () => assert.fail('banner rendering failed'),
  });
  await ctx.checkNewImages();
  assert.equal(banner.style.display, 'flex');
  assert.equal(cta.style.display, 'none');
  assert.match(msg.textContent, /working locally and not checked/);
  assert.equal(msg.title, '/nas/photos');
  assert.equal(banner.dataset.offline, 'local:/nas/photos');
  assert.deepEqual(removed, []); // an excluded root is not a fully checked zero
  dismissed = true;
  await ctx.checkNewImages();
  assert.equal(banner.style.display, 'none');
  localOnly = false;
  await ctx.checkNewImages();
  assert.equal(banner.style.display, 'flex');
  assert.match(msg.textContent, /offline/);
});

(async () => {
let failed = 0;
for (const [name, body] of tests) {
  try {
    await body();
  } catch (err) {
    failed++;
    console.error('FAIL ' + name + '\n' + (err && err.stack || err));
  }
}
if (failed) {
  console.error(failed + ' of ' + tests.length + ' navbar tests failed');
  process.exitCode = 1;
} else {
  console.log(tests.length + ' navbar tests passed');
}

})();
