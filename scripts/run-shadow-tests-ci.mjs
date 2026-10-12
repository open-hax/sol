import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const shadowCli = fileURLToPath(new URL('../node_modules/shadow-cljs/cli/runner.js', import.meta.url));

/** Decide success from completed suite counters and the compiler's termination. */
function resultOf(output, code, signal, timedOut) {
  if (timedOut) return { code: 1, reason: 'timeout' };
  if (signal || code !== 0) return { code: code || 1, reason: 'child-failed' };
  const counters = [...output.matchAll(/\b(\d+) failures?,\s*(\d+) errors?\./g)];
  const suites = [...output.matchAll(/\bRan (\d+) tests? containing (\d+) assertions?\.\s+(\d+) failures?,\s*(\d+) errors?\./g)];
  if (!suites.length || suites.some((m) => Number(m[1]) === 0 || Number(m[2]) === 0)) {
    return { code: 1, reason: 'missing-or-empty-suite' };
  }
  if (counters.some((m) => Number(m[1]) > 0 || Number(m[2]) > 0)) {
    return { code: 1, reason: 'failing-counters' };
  }
  if ([...output.matchAll(/\b(\d+) warnings?\b/g)].some((m) => Number(m[1]) > 0)) {
    return { code: 1, reason: 'compiler-warnings' };
  }
  return { code: 0, reason: 'completed-nonempty-suite' };
}


/**
 * Windows keeps a dead parent's PID in each orphan's ParentProcessId, so walk
 * that recorded tree and stop every descendant even after the leader exited
 * (`taskkill /T` can no longer traverse from a gone PID).
 */
function killWindowsDescendants(pid, stderr) {
  const script = [
    `$queue = [System.Collections.Generic.Queue[int]]::new(); $queue.Enqueue(${Number(pid)})`,
    '$all = Get-CimInstance Win32_Process | Select-Object ProcessId, ParentProcessId',
    'while ($queue.Count) { $p = $queue.Dequeue(); foreach ($c in $all | Where-Object { $_.ParentProcessId -eq $p }) {',
    '  $queue.Enqueue([int]$c.ProcessId); Stop-Process -Id $c.ProcessId -Force -ErrorAction SilentlyContinue } }',
  ].join('\n');
  const result = spawnSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', script], { stdio: 'ignore' });
  if (result.error || result.status !== 0) {
    stderr(`[sol-test] Could not reap Windows descendants of ${pid}: ${result.error?.message ?? `exit ${result.status}`}\n`);
  }
}

/** Kill the spawned compiler and any descendants it still owns. */
function killCompilerTree(pid, grouped, leaderAlive, stderr) {
  if (!pid) return;
  try {
    if (grouped) {
      process.kill(-pid, 'SIGKILL');
      return;
    }
    if (process.platform === 'win32') {
      // No POSIX process groups on Windows. taskkill /T works only while the
      // leader is alive; afterwards walk the recorded parent links instead.
      if (leaderAlive) spawnSync('taskkill', ['/pid', String(pid), '/T', '/F'], { stdio: 'ignore' });
      killWindowsDescendants(pid, stderr);
      return;
    }
    process.kill(pid, 'SIGKILL');
  } catch (error) {
    if (error.code !== 'ESRCH') stderr(`[sol-test] ${error.message}\n`);
  }
}

/** Run the local compiler with a bounded lifetime; return its verified verdict. */
export function runShadowTests({
  command = process.execPath,
  args = [shadowCli, 'compile', 'test'],
  timeoutMs = 180_000,
  // After the leader exits and its tree is reaped, wait at most this long for
  // the pipes to close before failing instead of hanging on an escaped holder.
  closeGraceMs = 10_000,
  stdout = (chunk) => process.stdout.write(chunk),
  stderr = (chunk) => process.stderr.write(chunk),
} = {}) {
  return new Promise((resolve) => {
    const grouped = process.platform !== 'win32';
    const child = spawn(command, args, {
      detached: grouped,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        CONTRACTS_DIR: process.env.CONTRACTS_DIR ?? 'test/fixtures/empty-contracts',
      },
    });
    let output = '';
    let timedOut = false;
    let leaderAlive = true;
    let settled = false;
    let graceTimer;
    const finish = (result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      clearTimeout(graceTimer);
      if (result.code !== 0) stderr(`[sol-test] Rejected test execution: ${result.reason}\n`);
      resolve(result);
    };
    const timeout = setTimeout(() => {
      timedOut = true;
      // Kill the owned compiler process group, including its JVM, on timeout.
      killCompilerTree(child.pid, grouped, leaderAlive, stderr);
    }, timeoutMs);
    const consume = (chunk, write) => {
      output += chunk.toString();
      write(chunk);
    };
    child.stdout.on('data', (chunk) => consume(chunk, stdout));
    child.stderr.on('data', (chunk) => consume(chunk, stderr));
    child.on('error', (error) => {
      stderr(`[sol-test] Failed to spawn compiler: ${error.message}\n`);
      finish({ code: 1, reason: 'spawn-failed' });
    });
    // Reap descendants as soon as the compiler leader exits: a JVM left in the
    // owned process group can hold the inherited pipes open and delay `close`.
    child.on('exit', () => {
      leaderAlive = false;
      killCompilerTree(child.pid, grouped, false, stderr);
      // Something outside the reaped tree still holds the pipes: stop reading
      // and fail explicitly rather than leave the promise pending forever.
      graceTimer = setTimeout(() => {
        child.stdout.destroy();
        child.stderr.destroy();
        finish({ code: 1, reason: 'descendant-held-pipes' });
      }, closeGraceMs);
    });
    // `close` still collects the final output; the verdict uses the leader's status.
    child.on('close', (code, signal) => {
      finish(resultOf(output, code, signal, timedOut));
    });
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  process.exitCode = (await runShadowTests()).code;
}
