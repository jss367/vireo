import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../../vireo/static/vireo-time.js', import.meta.url), 'utf8');

function harness(format = 'system', locale = 'en-US') {
  class LocaleDate extends Date {
    toLocaleTimeString(locales, options) { return super.toLocaleTimeString(locale, options); }
    toLocaleString(locales, options) { return super.toLocaleString(locale, options); }
  }
  const window = {VIREO_TIME_FORMAT: format};
  vm.runInNewContext(source, {window, Date: LocaleDate});
  return {api: window.VireoTime, Date: LocaleDate};
}

test('system default follows the locale, including when switching back from an override', () => {
  for (const locale of ['en-US', 'en-GB']) {
    const {api, Date: LocalDate} = harness('system', locale);
    const date = new LocalDate('2026-10-20T15:45:30Z');
    assert.equal(api.time(date), date.toLocaleTimeString());
    assert.equal(api.dateTime(date), date.toLocaleString());
    api.setFormat('24h');
    api.setFormat('system');
    assert.equal(api.time(date), date.toLocaleTimeString());
    api.setFormat('invalid');
    assert.equal(api.time(date), date.toLocaleTimeString());
  }
});

test('12-hour and 24-hour overrides handle midnight, noon, and seconds in either locale', () => {
  for (const locale of ['en-US', 'en-GB']) {
    const {api, Date: LocalDate} = harness('12h', locale);
    for (const hour of ['00', '12', '15', '23']) {
      const date = new LocalDate(`2026-10-20T${hour}:45:30Z`);
      const options = {hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'UTC'};
      for (const [format, cycle] of [['12h', 'h12'], ['24h', 'h23']]) {
        api.setFormat(format);
        assert.equal(api.time(date, options), date.toLocaleTimeString([], {...options, hourCycle: cycle}));
        assert.equal(api.dateTime(date, options), date.toLocaleString([], {...options, hourCycle: cycle}));
      }
    }
    assert.equal(api.captureTime('2026-10-20T00:00:00'), '00:00:00');
    assert.equal(api.captureTime('2026-10-20T12:00:00'), '12:00:00');
    api.setFormat('12h');
    assert.match(api.captureTime('2026-10-20T00:00:00'), /^12:00:00\s?[Aa][Mm]$/);
    assert.match(api.captureTime('2026-10-20T12:00:00'), /^12:00:00\s?[Pp][Mm]$/);
  }
});

test('photo times preserve camera date and clock across offsets and DST gaps', () => {
  const {api} = harness('24h');
  for (const value of ['2026-03-08T02:30:45', '2026-03-08T02:30:45-08:00',
    '2026-03-08 02:30:45.123456+14:00', '2026-03-08T02:30:45Z']) {
    assert.equal(api.captureDateTime(value), '2026-03-08 02:30');
    assert.equal(api.captureDateTime(value, true), '2026-03-08 02:30:45');
  }
  assert.equal(api.captureTime('2026-11-01T01:30:10-07:00'), '01:30:10');
  assert.equal(api.captureTime('2026-10-20T15:45'), '15:45');
  assert.equal(api.captureDateTime('2026-10-20'), '2026-10-20');
  assert.equal(api.captureDateTime('unknown'), 'unknown');
  assert.equal(api.captureDateTime(null), '');
});

test('localized photo dates retain their calendar date and requested date style', () => {
  for (const locale of ['en-US', 'en-GB']) {
    const {api, Date: LocalDate} = harness('12h', locale);
    const options = {month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit'};
    const expected = new LocalDate('2026-10-20T00:45:00Z').toLocaleString([], {
      ...options, timeZone: 'UTC', hourCycle: 'h12',
    });
    assert.equal(api.captureDateTime('2026-10-20T00:45:00+14:00', false, options), expected);
  }
});
