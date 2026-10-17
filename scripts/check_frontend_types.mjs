import {execFileSync} from 'node:child_process';
import {readdirSync, readFileSync} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {parse} from 'acorn';

const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const compiler = path.join(repository, 'node_modules/typescript/bin/tsc');
const requiredOptions = ['allowJs', 'checkJs', 'strict', 'noUncheckedIndexedAccess', 'noEmit'];
const strictOptions = ['noImplicitAny', 'strictNullChecks', 'strictFunctionTypes',
  'strictBindCallApply', 'strictPropertyInitialization', 'noImplicitThis',
  'useUnknownInCatchVariables', 'alwaysStrict', 'strictBuiltinIteratorReturn'];

function walk(directory) {
  return readdirSync(directory, {withFileTypes: true}).flatMap(entry => {
    const filename = path.join(directory, entry.name);
    if (entry.isDirectory()) return entry.name === 'vendor' ? [] : walk(filename);
    return [filename];
  });
}

function compile(project, extra = []) {
  return execFileSync(process.execPath, [compiler, '--project', project, ...extra],
    {encoding: 'utf8', maxBuffer: 32 * 1024 * 1024});
}

function checkedFiles(project) {
  const configuration = JSON.parse(compile(project, ['--showConfig']));
  const options = configuration.compilerOptions;
  if (options.noCheck === true) throw new Error(`${project}: noCheck cannot be enabled`);
  for (const option of requiredOptions) {
    if (options[option] !== true) throw new Error(`${project}: ${option} must be true`);
  }
  for (const option of strictOptions) {
    if (options[option] === false) throw new Error(`${project}: ${option} cannot be disabled`);
  }
  return configuration.files.map(filename => path.resolve(path.dirname(project), filename));
}

function rejectSuppressions(filename) {
  const comments = [];
  parse(readFileSync(filename, 'utf8'), {
    ecmaVersion: 'latest', sourceType: 'script', onComment: comments,
  });
  for (const comment of comments) {
    if (/@ts-(?:nocheck|ignore|expect-error)\b/.test(comment.value)) {
      throw new Error(`${filename}: first-party implementations must not suppress type errors`);
    }
  }
}

/** Check project options, implementation coverage, then each actual program. */
export function checkFrontendTypes(root = repository, {run = true} = {}) {
  const projects = [path.join(root, 'tsconfig.frontend.json'),
    ...walk(path.join(root, 'tsconfig.frontend-pages')).filter(filename => filename.endsWith('.json'))];
  const checked = new Set(projects.flatMap(checkedFiles));
  const sources = walk(path.join(root, 'vireo/static'))
    .filter(filename => filename.endsWith('.js') && !filename.endsWith('.min.js'));
  const missing = sources.filter(filename => !checked.has(filename));
  if (missing.length) throw new Error(`Strict frontend projects omit ${missing.length} scripts:\n${missing.join('\n')}`);
  sources.forEach(rejectSuppressions);
  if (run) for (const project of projects) compile(project);
  return {projects: projects.length, scripts: sources.length};
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const result = checkFrontendTypes();
    console.log(`Strict frontend checking passed: ${result.scripts} scripts in ${result.projects} projects.`);
  } catch (error) {
    console.error(error.stdout?.toString() || error.message);
    process.exitCode = 1;
  }
}
