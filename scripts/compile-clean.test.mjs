// SPDX-License-Identifier: GPL-3.0-or-later
// The Node 22 runtime workflow must apply the evidence workflow's compiler guard
// to both builds, so a warning or missing artifact cannot leave the job green.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

const script = new URL('./compile-clean.sh', import.meta.url).pathname;
// compile-clean.sh is a Bash guard run only by the Ubuntu runtime workflow;
// Windows has no /bin/bash, so its behavioral fixtures are POSIX-only.
const bashFixture = { skip: process.platform === 'win32' && 'Bash-only compiler guard (Ubuntu workflow)' };
const runtimeWorkflow = readFileSync(new URL('../.github/workflows/node22-runtime.yml', import.meta.url), 'utf8');

function run(output, { code = 0, artifact = true, stale = false, channel = 'stdout', target = 'server' } = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'sol-compile-clean-'));
  try {
    const bundle = join(directory, 'bundle.js');
    // A bundle left by an earlier build; the guard must not accept it.
    if (stale) writeFileSync(bundle, '// stale fixture');
    // The fixture build writes the artifact itself, as the real compiler does.
    const write = artifact ? `require('node:fs').writeFileSync(${JSON.stringify(bundle)}, '// fixture'); ` : '';
    const payload = `${write}process.${channel}.write(${JSON.stringify(output)}); process.exitCode = ${code};`;
    return spawnSync('/bin/bash', ['--noprofile', '--norc', script, target, bundle, process.execPath, '-e', payload],
      { env: { ...process.env, RUNNER_TEMP: directory }, encoding: 'utf8' });
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

const compiled = '[:server] Build completed. (197 files, 9 compiled, 0 warnings, 1.0s)\n';
test('complete build with nonempty artifact passes', bashFixture, () => assert.equal(run(compiled).status, 0));
for (const [label, output, options] of [
  ['compiler warning', compiled.replace('0 warnings', '1 warnings'), {}],
  ['stderr warning despite zero completion counter', compiled + 'WARNING: fixture\n', { channel: 'stderr' }],
  ['missing completion', 'Compiler exited quietly\n', {}],
  ['wrong completed target', compiled.replace('server', 'clio-native'), {}],
  ['missing artifact', compiled, { artifact: false }],
  ['stale artifact the build did not write', compiled, { artifact: false, stale: true }],
  ['nonzero child exit after completion', compiled, { code: 7 }],
]) {
  test(`reject ${label}`, bashFixture, () => assert.notEqual(run(output, options).status, 0));
}

test('Node 22 runtime workflow guards both builds', () => {
  assert.match(runtimeWorkflow, /bash scripts\/compile-clean\.sh server dist\/server\.js npm run build\n/);
  assert.match(runtimeWorkflow,
    /bash scripts\/compile-clean\.sh clio-native target\/clio-native\/probe\.js npm run build:clio-native\n/);
  for (const line of runtimeWorkflow.split('\n')) {
    if (/npm run build/.test(line)) assert.match(line, /scripts\/compile-clean\.sh/, `unguarded build: ${line.trim()}`);
  }
});
