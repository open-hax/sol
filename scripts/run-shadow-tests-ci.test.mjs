import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import test from 'node:test';
import { mkdtempSync, readFileSync, rmSync, symlinkSync } from 'node:fs';
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
  // The timed-out leader starts a descendant and reports its PID, so the
  // timeout path must stop the whole compiler tree, not just the leader.
  const leader = [
    "const { spawn } = require('node:child_process');",
    "const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000);'], { stdio: 'ignore' });",
    "process.stdout.write('CHILD ' + child.pid + '\\n');",
    'setInterval(() => {}, 1000);',
  ].join(' ');
  let captured = '';
  const result = await runShadowTests({
    command: process.execPath,
    args: ['-e', leader],
    timeoutMs: 1_000,
    stdout: (chunk) => { captured += chunk.toString(); },
    stderr: quiet,
  });
  assert.notEqual(result.code, 0);
  assert.equal(result.reason, 'timeout');
  const descendant = Number(/CHILD (\d+)/.exec(captured)?.[1]);
  assert.ok(descendant > 0, `timed-out leader did not report its descendant: ${captured}`);
  let stopped = processStopped(descendant);
  for (let i = 0; i < 50 && !stopped; i += 1) {
    await new Promise((r) => setTimeout(r, 100));
    stopped = processStopped(descendant);
  }
  if (!stopped) try { process.kill(descendant, 'SIGKILL'); } catch {}
  assert.equal(stopped, true, `descendant ${descendant} survived the timeout`);
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

// A killed compiler can stay a zombie (state Z) when PID 1 does not reap
// adopted children, and signal 0 still succeeds for a zombie. Treat that
// state as stopped on Linux; elsewhere fall back to signal 0 alone.
function processStopped(pid) {
  try { process.kill(pid, 0); } catch { return true; }
  if (process.platform !== 'linux') return false;
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, 'utf8');
    return stat.slice(stat.lastIndexOf(')') + 2).startsWith('Z');
  } catch (error) {
    // ENOENT means the pid exited between the two checks. Any other read
    // failure (procfs hidden or restricted) proves nothing, so keep polling.
    return error.code === 'ENOENT';
  }
}

test('a SIGTERM or SIGINT to the runner kills the compiler group', {
  timeout: 20_000,
  // POSIX only: the compiler runs in its own process group there, so a
  // terminal signal to the runner never reaches it on its own.
  skip: process.platform === 'win32',
}, async () => {
  const runnerUrl = new URL('./run-shadow-tests-ci.mjs', import.meta.url).href;
  for (const signal of ['SIGTERM', 'SIGINT']) {
    // The compiler also starts a child in its own group, so the test fails if
    // the handler stops the group leader but leaves a descendant running.
    const compiler = [
      "const { spawn } = require('node:child_process');",
      "const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000);'], { stdio: 'ignore' });",
      "process.stdout.write('COMPILER ' + process.pid + ' CHILD ' + child.pid + '\\n');",
      "setInterval(() => {}, 1000);",
    ].join(' ');
    const wrapper = [
      `const { runShadowTests } = await import(${JSON.stringify(runnerUrl)});`,
      `await runShadowTests({ command: process.execPath, args: ['-e', ${JSON.stringify(compiler)}], timeoutMs: 60_000 });`,
    ].join(' ');
    const runner = spawn(process.execPath, ['--input-type=module', '-e', wrapper], { stdio: ['ignore', 'pipe', 'ignore'] });
    let seen = '';
    const pids = await new Promise((resolve, reject) => {
      runner.stdout.on('data', (chunk) => {
        seen += chunk.toString();
        const match = /COMPILER (\d+) CHILD (\d+)/.exec(seen);
        if (match) resolve({ compiler: Number(match[1]), child: Number(match[2]) });
      });
      runner.on('exit', () => reject(new Error(`runner exited before the compiler started: ${seen}`)));
    });
    const exited = new Promise((resolve) => runner.on('exit', (code, sig) => resolve({ code, sig })));
    runner.kill(signal);
    const outcome = await exited;
    const survivors = [];
    for (const [label, pid] of Object.entries(pids)) {
      let alive = true;
      for (let i = 0; i < 50 && alive; i += 1) {
        if (processStopped(pid)) alive = false;
        else await new Promise((r) => setTimeout(r, 100));
      }
      if (alive) {
        survivors.push(`${label} ${pid}`);
        try { process.kill(pid, 'SIGKILL'); } catch {}
      }
    }
    assert.deepEqual(survivors, [], `survived ${signal} to the runner: ${survivors.join(', ')}`);
    assert.equal(outcome.sig, signal);
  }
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
