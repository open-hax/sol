import { spawn } from 'node:child_process';
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

/** Run the local compiler with a bounded lifetime; return its verified verdict. */
export function runShadowTests({
  command = process.execPath,
  args = [shadowCli, 'compile', 'test'],
  timeoutMs = 180_000,
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
    const timeout = setTimeout(() => {
      timedOut = true;
      // Kill the owned compiler process group, including its JVM, on timeout.
      try {
        if (grouped && child.pid) process.kill(-child.pid, 'SIGKILL');
        else child.kill('SIGKILL');
      } catch (error) {
        if (error.code !== 'ESRCH') stderr(`[sol-test] ${error.message}\n`);
      }
    }, timeoutMs);
    const consume = (chunk, write) => {
      output += chunk.toString();
      write(chunk);
    };
    child.stdout.on('data', (chunk) => consume(chunk, stdout));
    child.stderr.on('data', (chunk) => consume(chunk, stderr));
    child.on('error', (error) => {
      clearTimeout(timeout);
      stderr(`[sol-test] Failed to spawn compiler: ${error.message}\n`);
      resolve({ code: 1, reason: 'spawn-failed' });
    });
    // Reap descendants as soon as the compiler leader exits: a JVM left in the
    // owned process group can hold the inherited pipes open and delay `close`.
    child.on('exit', () => {
      if (grouped && child.pid) {
        try {
          process.kill(-child.pid, 'SIGKILL');
        } catch (error) {
          if (error.code !== 'ESRCH') stderr(`[sol-test] ${error.message}\n`);
        }
      }
    });
    // `close` still collects the final output; the verdict uses the leader's status.
    child.on('close', (code, signal) => {
      clearTimeout(timeout);
      const result = resultOf(output, code, signal, timedOut);
      if (result.code !== 0) stderr(`[sol-test] Rejected test execution: ${result.reason}\n`);
      resolve(result);
    });
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  process.exitCode = (await runShadowTests()).code;
}
