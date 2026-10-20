const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {test} = require('node:test');
const vm = require('node:vm');

const source = readFileSync('vireo/static/settings/system-tools.js', 'utf8');
const snippetIds = ['mcpClaudeCodeCommand', 'mcpClaudeDesktopJson', 'mcpCodexCommand', 'mcpCodexToml'];

function harness(safeFetch) {
  const elements = Object.fromEntries([...snippetIds, 'mcpCommandShell', 'mcpCheckButton', 'mcpCheckResult']
    .map(id => [id, {textContent: '', disabled: false}]));
  const copied = [];
  const ctx = vm.createContext({document: {getElementById: id => elements[id]}, safeFetch,
    navigator: {clipboard: {writeText: async text => copied.push(text)}}, showToast() {}});
  vm.runInContext(source, ctx);
  return {ctx, elements, copied};
}

test('loads and copies Codex setup alongside existing Claude snippets', async () => {
  const data = {claude_code: 'claude mcp add vireo -- "/My Photos/Vireo" --mcp',
    claude_desktop: {mcpServers: {vireo: {args: ['--mcp'], command: '/My Photos/Vireo'}}},
    codex: {command: 'codex mcp add vireo -- "/My Photos/Vireo" --mcp',
      config_toml: '[mcp_servers.vireo]\ncommand = "/My Photos/Vireo"\nargs = ["--mcp"]\n'}, shell: 'PowerShell'};
  const h = harness(async url => { assert.equal(url, '/api/system/mcp-config'); return data; });
  await h.ctx.loadMcpConnectInfo();
  assert.equal(h.elements.mcpClaudeCodeCommand.textContent, data.claude_code);
  assert.deepEqual(JSON.parse(h.elements.mcpClaudeDesktopJson.textContent), data.claude_desktop);
  assert.equal(h.elements.mcpCodexCommand.textContent, data.codex.command);
  assert.equal(h.elements.mcpCodexToml.textContent, data.codex.config_toml);
  assert.equal(h.elements.mcpCommandShell.textContent, 'PowerShell');
  for (const id of snippetIds) h.ctx.copyMcpSnippet(id);
  assert.deepEqual(h.copied, snippetIds.map(id => h.elements[id].textContent));
});

test('setup failure replaces every loading placeholder without echoing sensitive errors', async () => {
  const h = harness(async () => { throw new Error('runtime-token-secret'); });
  await h.ctx.loadMcpConnectInfo();
  for (const id of snippetIds) {
    assert.match(h.elements[id].textContent, /Reload Settings/);
    assert(!h.elements[id].textContent.includes('runtime-token-secret'));
  }
});

test('a successful server check identifies the catalog and asks for client verification', async () => {
  let release, requests = 0;
  const h = harness((url, options) => {
    assert.equal(url, '/api/system/mcp-check');
    assert.equal(options.method, 'POST');
    requests++;
    return new Promise(resolve => { release = resolve; });
  });
  const pending = h.ctx.checkMcpConnection();
  assert.equal(h.elements.mcpCheckButton.disabled, true);
  await h.ctx.checkMcpConnection();
  assert.equal(requests, 1);
  release({ok: true, status: {active_workspace: {id: 7, name: 'Birds'}, photo_count: 12},
    folders: [{path: '/My Photos/Birds'}]});
  await pending;
  assert.equal(h.elements.mcpCheckButton.disabled, false);
  const text = h.elements.mcpCheckResult.textContent;
  assert.match(text, /server started and answered read-only tools/);
  assert.match(text, /Workspace: Birds \(id 7\)/);
  assert.match(text, /Photos: 12/);
  assert.match(text, /\/My Photos\/Birds/);
  assert.match(text, /assistant’s own MCP status/);
  assert(!text.includes('assistant connected'));
});

test('an actionable server failure permits retry', async () => {
  const h = harness(async () => ({ok: false, message: 'Open Vireo and retry.'}));
  await h.ctx.checkMcpConnection();
  assert.equal(h.elements.mcpCheckButton.disabled, false);
  assert.equal(h.elements.mcpCheckResult.textContent, 'Server check failed: Open Vireo and retry.');
});

test('transport failures never echo raw exception text', async () => {
  const h = harness(async () => { throw new Error('runtime-token-secret'); });
  await h.ctx.checkMcpConnection();
  assert.equal(h.elements.mcpCheckButton.disabled, false);
  assert.match(h.elements.mcpCheckResult.textContent, /reload Settings and retry/);
  assert(!h.elements.mcpCheckResult.textContent.includes('runtime-token-secret'));
});
