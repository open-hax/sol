import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, rmSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { isDirectRun, runShadowTests } from './run-shadow-tests-ci.mjs';

const passing = 'Ran 2 tests containing 3 assertions.\n0 failures, 0 errors.\n';
const failing = 'Ran 2 tests containing 3 assertions.\n1 failures, 0 errors.\n';
const quiet = () => {};

/** Exercise the process boundary using controlled output and a real child exit. */
function childResult(output, exit = 0) {
  return runShadowTests({
    command: process.execPath,
    args: ['-e', `process.stdout.write(${JSON.stringify(output)}); process.exitCode = ${exit};`],
    stdout: quiet,
    stderr: quiet,
  });
}

test('a completed nonempty suite passes', async () => {
  assert.equal((await childResult(passing)).code, 0);
});

test('compiler exit zero after an import crash fails', async () => {
  assert.notEqual((await childResult('Error: Cannot find module sdk\nBuild completed.')).code, 0);
});

test('zero tests or zero assertions never qualify', async () => {
  for (const output of [
    'Ran 0 tests containing 0 assertions.\n0 failures, 0 errors.\n',
    'Ran 2 tests containing 0 assertions.\n0 failures, 0 errors.\n',
  ]) assert.notEqual((await childResult(output)).code, 0);
});

test('a later failing summary cannot be hidden by an earlier pass', async () => {
  assert.notEqual((await childResult(passing + failing)).code, 0);
  assert.notEqual((await childResult(failing + passing)).code, 0);
});

test('errors and unpaired failing counters fail', async () => {
  assert.notEqual((await childResult('Ran 1 test containing 1 assertion.\n0 failures, 1 error.')).code, 0);
  assert.notEqual((await childResult(passing + '1 failure, 0 errors.')).code, 0);
});

test('missing summary and nonzero child exit fail', async () => {
  assert.notEqual((await childResult('0 failures, 0 errors.')).code, 0);
  assert.notEqual((await childResult(passing, 7)).code, 0);
});

test('compiler warnings cannot qualify a passing suite', async () => {
  for (const label of ['warning', 'warnings']) {
    assert.notEqual((await childResult(passing + `Build completed. (1 files, 1 compiled, 1 ${label})`)).code, 0);
  }
});

test('a signalled child fails even after passing counters', async () => {
  const result = await runShadowTests({
    command: process.execPath,
    args: ['-e', `process.stdout.write(${JSON.stringify(passing)}); process.kill(process.pid, 'SIGTERM');`],
    stdout: quiet,
    stderr: quiet,
  });
  assert.notEqual(result.code, 0);
});

test('missing executable and timed out child fail', async () => {
  assert.notEqual((await runShadowTests({ command: '/sol-test-no-such-executable', args: [], stdout: quiet, stderr: quiet })).code, 0);
  const result = await runShadowTests({
    command: process.execPath,
    args: ['-e', 'setInterval(() => {}, 1000);'],
    timeoutMs: 100,
    stdout: quiet,
    stderr: quiet,
  });
  assert.notEqual(result.code, 0);
  assert.equal(result.reason, 'timeout');
});

test('a descendant holding stdio open is reaped when the compiler exits', async () => {
  const leader = [
    "const { spawn } = require('node:child_process');",
    "spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000);'], { stdio: 'inherit' }).unref();",
    `process.stdout.write(${JSON.stringify(passing)}, () => process.exit(0));`,
  ].join(' ');
  const result = await runShadowTests({
    command: process.execPath,
    args: ['-e', leader],
    timeoutMs: 5_000,
    stdout: quiet,
    stderr: quiet,
  });
  assert.equal(result.reason, 'completed-nonempty-suite');
  assert.equal(result.code, 0);
});

test('a pipe holder outside the reaped tree fails instead of hanging', {
  timeout: 15_000,
  // POSIX process-group behavior only: on Windows the detached grandchild is
  // still found by ParentProcessId and reaped, which the previous test covers.
  skip: process.platform === 'win32',
}, async () => {
  // The grandchild starts its own process group, so killing the compiler's
  // group cannot reach it, as with a Windows orphan that escaped taskkill.
  const leader = [
    "const { spawn } = require('node:child_process');",
    "const g = spawn(process.execPath, ['-e', 'setTimeout(() => {}, 20000);'], { stdio: 'inherit', detached: true });",
    "process.stderr.write('GRANDCHILD ' + g.pid + '\\n'); g.unref();",
    `process.stdout.write(${JSON.stringify(passing)}, () => process.exit(0));`,
  ].join(' ');
  let captured = '';
  const result = await runShadowTests({
    command: process.execPath,
    args: ['-e', leader],
    timeoutMs: 60_000,
    closeGraceMs: 300,
    stdout: quiet,
    stderr: (chunk) => { captured += chunk.toString(); },
  });
  const grandchild = Number(/GRANDCHILD (\d+)/.exec(captured)?.[1]);
  if (grandchild) try { process.kill(grandchild, 'SIGKILL'); } catch {}
  assert.equal(result.code, 1);
  assert.equal(result.reason, 'descendant-held-pipes');
});

test('direct execution through a symlinked path is still recognised', () => {
  const directory = mkdtempSync(join(tmpdir(), 'sol-runner-'));
  try {
    const runner = fileURLToPath(new URL('./run-shadow-tests-ci.mjs', import.meta.url));
    const alias = join(directory, 'run-shadow-tests-ci.mjs');
    symlinkSync(runner, alias);
    // import.meta.url is symlink-resolved; argv[1] keeps the alias spelling.
    assert.equal(isDirectRun(alias, pathToFileURL(runner).href), true);
    assert.equal(isDirectRun(runner, pathToFileURL(runner).href), true);
    assert.equal(isDirectRun(undefined, pathToFileURL(runner).href), false);
    assert.equal(isDirectRun(join(directory, 'missing.mjs'), pathToFileURL(runner).href), false);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
