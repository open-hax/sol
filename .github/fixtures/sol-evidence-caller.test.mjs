// SPDX-License-Identifier: GPL-3.0-or-later
// Test only the caller's orchestration guards. Sol's existing test guard remains authoritative.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

const workflow = readFileSync(new URL('../workflows/eta-mu-evidence-review.yml', import.meta.url), 'utf8');
function block(label) {
  return workflow.split(`# BEGIN ${label}\n`)[1].split(`# END ${label}`)[0]
    .split('\n').map(line => line.slice(8)).join('\n');
}
const script = workflow.split('      evidence_gates_script: |\n')[1]
  .split('\n').map(line => line.slice(8)).join('\n');
function scratch(action) {
  const directory = mkdtempSync(join(tmpdir(), 'sol-caller-'));
  try { return action(directory); } finally { rmSync(directory, { recursive: true, force: true }); }
}
function wrapped(command, output, code = 0, artifact = true, channel = 'stdout', stale = false) {
  return scratch(directory => {
    const bundle = join(directory, 'bundle.js');
    // The child command writes the artifact, as a real build does; a stale
    // bundle left by an earlier build must not satisfy the guard.
    if (stale) writeFileSync(bundle, '// stale fixture');
    const write = artifact ? `require('node:fs').writeFileSync(${JSON.stringify(bundle)}, '// fixture'); ` : '';
    const payload = `${write}process.${channel}.write(${JSON.stringify(output)}); process.exitCode = ${code};`;
    const args = command === 'compile_clean' ? ['server', bundle] : ['guard'];
    return spawnSync('/bin/bash', ['--noprofile', '--norc', '-c', `${block('GATE_WRAPPERS')}\n${command} "$@"`, 'fixture',
      ...args, process.execPath, '-e', payload], { env: { ...process.env, RUNNER_TEMP: directory }, encoding: 'utf8' });
  });
}
const compiled = '[:server] Build completed. (197 files, 9 compiled, 0 warnings, 1.0s)\n';
test('complete build with nonempty artifact passes', () => assert.equal(wrapped('compile_clean', compiled).status, 0));
for (const [label, output, code, artifact, channel, stale] of [
  ['compiler warning', compiled.replace('0 warnings', '1 warnings'), 0, true],
  ['stderr warning despite zero completion counter', compiled + 'WARNING: fixture\n', 0, true, 'stderr'],
  ['missing completion', 'Compiler exited quietly\n', 0, true],
  ['wrong completed target', compiled.replace('server', 'clio-native'), 0, true],
  ['missing artifact', compiled, 0, false],
  ['nonzero child exit after completion', compiled, 7, true],
  ['stale artifact the build did not write', compiled, 0, false, 'stdout', true],
]) {
  test(`reject ${label}`, () => assert.notEqual(wrapped('compile_clean', output, code, artifact, channel, stale).status, 0));
}
const tap = '# tests 13\n# pass 13\n# fail 0\n';
test('completed nonempty Node guards pass', () => assert.equal(wrapped('node_tests', tap).status, 0));
for (const [label, output] of [
  ['empty Node suite', '# tests 0\n# pass 0\n# fail 0\n'],
  ['missing Node counters', 'no tests ran\n'],
  ['partially skipped Node suite', '# tests 13\n# pass 12\n# fail 0\n'],
  ['duplicate contradictory counters', tap + '# tests 0\n# pass 0\n# fail 0\n'],
]) {
  test(`reject ${label}`, () => assert.notEqual(wrapped('node_tests', output).status, 0));
}
test('entire inherited evidence snippet is valid Bash', () => {
  const result = spawnSync('/bin/bash', ['--noprofile', '--norc', '-n'], { input: script, encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
});
test('recorded install failure cannot become successful dependent gates', () => scratch(directory => {
  const statuses = join(directory, 'statuses');
  const log = join(directory, 'log');
  writeFileSync(statuses, ''); writeFileSync(log, '');
  const mock = 'run_gate() { local code=0; if [[ "$1" == install ]]; then code=7; fi; printf "%s=%s\\n" "$1" "$code" >> "$statuses"; return 0; }\n';
  const result = spawnSync('/bin/bash', ['--noprofile', '--norc', '-c', mock + script],
    { env: { ...process.env, statuses, log, RUNNER_TEMP: directory }, encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  const records = Object.fromEntries(readFileSync(statuses, 'utf8').trim().split('\n').map(line => line.split('=')));
  assert.equal(records.install, '7');
  for (const gate of ['test', 'build', 'clio_native_build', 'production_install', 'production_native', 'startup_default', 'startup_clio']) {
    assert.equal(records[gate], '125', gate);
  }
}));
test('generated cache exclusion preserves tracked modifications and source visibility', () => scratch(directory => {
  function git(...args) {
    const result = spawnSync('git', args, { cwd: directory, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr); return result.stdout;
  }
  git('init', '-q');
  mkdirSync(join(directory, '.clj-kondo', '.cache'), { recursive: true });
  writeFileSync(join(directory, '.clj-kondo', '.cache', 'tracked'), 'before');
  git('add', '.clj-kondo/.cache/tracked');
  writeFileSync(join(directory, '.clj-kondo', '.cache', 'tracked'), 'after');
  writeFileSync(join(directory, '.clj-kondo', '.cache', 'generated'), 'cache');
  writeFileSync(join(directory, 'source.cljs'), '(ns fixture)');
  const result = spawnSync('/bin/bash', ['--noprofile', '--norc', '-e', '-o', 'pipefail', '-c', block('GENERATED_EXCLUDES')],
    { cwd: directory, encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  const status = git('status', '--porcelain');
  assert.match(status, /^AM .clj-kondo\/.cache\/tracked$/m);
  assert.match(status, /^\?\? source.cljs$/m);
  assert.doesNotMatch(status, /generated/);
}));
