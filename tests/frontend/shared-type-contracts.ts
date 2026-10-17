// Check public utility callers as well as the implementation bodies.
const api = window.Vireo?.api;
if (api) {
  const result: Promise<{job_id: string}> = api.json<{job_id: string}>('/api/jobs');
  void result;
  // Untyped JSON remains unknown until the caller names or validates its shape.
  const raw = api.json('/api/jobs');
  // @ts-expect-error unknown JSON cannot be used as a job response
  const untyped: Promise<{job_id: string}> = raw;
  void untyped;
  // @ts-expect-error fetch options must use the browser RequestInit contract
  api.fetch('/api/jobs', {method: 123});
}

window.Keymap.register('browse', {key: 'ctrl+a', action: event => event.preventDefault()});
// @ts-expect-error actions receive keyboard events
window.Keymap.register('browse', {key: 'ctrl+a', action: (photo: number) => photo});
// @ts-expect-error shortcut keys must be strings
window.Keymap.parseShortcut(123);

VireoViewPreferences.restore(document.createElement('select'));
// @ts-expect-error restore accepts DOM elements, not element IDs
VireoViewPreferences.restore('sort');
// @ts-expect-error map status requires coverage counters and photo rows
mapStatusHtml({photos: []});
