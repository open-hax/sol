// A temporary localhost probe of the existing server, never a service launcher.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdir, mkdtemp, rm, readdir, readFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const bundle = resolve(process.argv[2] ?? 'dist/server.js');
const clioEnabled = process.argv[3] === '--clio';
await mkdir('.ημ', { recursive: true });
const scratch = await mkdtemp(resolve('.ημ/startup-'));
await mkdir(join(scratch, 'contracts'));
let output = '';
let closed = false;
let exit;
let spawnError;
const child = spawn(process.execPath, [bundle], {
  cwd: scratch,
  env: {
    SOL_HOST: '127.0.0.1',
    SOL_PORT: '0',
    CONTRACTS_DIR: join(scratch, 'contracts'),
    WORKSPACE_ROOT: scratch,
    ...(clioEnabled ? {
      SOL_CLIO_LEDGER_FILE: join(scratch, 'clio-epoch', 'events.edn'),
      SOL_CLIO_SCHEMA_DIR: join(scratch, 'clio-epoch', 'schemas'),
      SOL_CLIO_INITIALIZE: 'true',
    } : {}),
  },
  stdio: ['ignore', 'pipe', 'pipe'],
});
child.stdout.on('data', (chunk) => { output += chunk.toString(); });
child.stderr.on('data', (chunk) => { output += chunk.toString(); });
child.on('error', (error) => { spawnError = error; });
child.on('close', (code, signal) => { closed = true; exit = { code, signal }; });

/** Bound readiness and shutdown waits while reporting process creation errors. */
async function waitUntil(predicate, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (spawnError) throw spawnError;
    if (Date.now() >= deadline) throw new Error('Sol startup/stop probe timed out');
    await delay(20);
  }
}

try {
  await waitUntil(() => closed || /http:\/\/127\.0\.0\.1:\d+/.test(output), 15_000);
  assert.equal(closed, false, `Server exited before listening: ${JSON.stringify(exit)}\n${output}`);
  const address = output.match(/http:\/\/127\.0\.0\.1:\d+/)[0];
  const response = await fetch(`${address}/health`, { signal: AbortSignal.timeout(3_000) });
  assert.equal(response.status, 200);
  const health = await response.json();
  assert.equal(health.status, 'ok');
  assert.equal(health.service, 'open-hax-sol-cljs');
  if (clioEnabled) {
    assert.equal(await readFile(join(scratch, 'clio-epoch', 'events.edn'), 'utf8'), '');
    const snapshots = await readdir(join(scratch, 'clio-epoch', 'schemas'));
    assert.equal(snapshots.length, 1);
    assert.match(snapshots[0], /^[0-9a-f]{64}\.edn$/);
  }
  assert.equal(child.kill('SIGTERM'), true);
  await waitUntil(() => closed, 5_000);
  assert.deepEqual(exit, { code: 0, signal: null });
  console.log(JSON.stringify({ node: process.version, clioEnabled, address, health, signal: 'SIGTERM', exit: 0 }));
} catch (error) {
  console.error(output);
  throw error;
} finally {
  try {
    if (!closed) {
      child.kill('SIGKILL');
      await waitUntil(() => closed, 5_000);
    }
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}
