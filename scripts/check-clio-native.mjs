// Test a compiled Sol/Clio ESM adapter with isolated files and no provider calls.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { copyFile, mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const bundle = resolve(process.env.SOL_CLIO_NATIVE_BUNDLE ?? process.argv[2] ?? 'target/clio-native/probe.js');
const scratch = await mkdtemp(join(tmpdir(), 'sol-clio-native-'));
try {
  const nativeMapped = async () =>
    process.platform !== 'linux' || /fs-ext/.test(await readFile('/proc/self/maps', 'utf8'));
  const { probe } = await import(pathToFileURL(bundle).href);
  // Clio and Sol load fs-ext-extra-prebuilt on the first lock, not at import.
  if (process.platform === 'linux') {
    assert.equal(await nativeMapped(), false, 'native lock binding loaded at import');
  }
  // The anchor must match the bundle this script imported, not a value
  // re-derived from the shared module-URL registry.
  const evidence = await probe(scratch, bundle);
  assert.equal(await nativeMapped(), true, 'first lock did not load the native binding');
  assert.match(evidence, /:status :passed/);
  assert.match(evidence, /:events 2/);
  assert.match(evidence, /:identical-retry true/);
  assert.match(evidence, /:reopen-replay true/);
  assert.match(evidence, /:stream-sequences \[1 2\]/);
  assert.match(evidence, /:module-anchor true/, 'probe bundle did not record its module URL');
  if (!process.env.SOL_CLIO_NATIVE_OUT_OF_TREE) {
    // Re-run from a cwd with no node_modules: the binding must still resolve
    // from Sol's install, not the caller's directory.
    // The host script is a copy outside Sol's tree too, so only the loaded
    // bundle's own location can reach Sol's node_modules (embedding case).
    const host = join(scratch, 'host-check.mjs');
    await copyFile(process.argv[1], host);
    // The bundle path goes through env, not argv, like an embedding host.
    const child = spawnSync(process.execPath, [host], {
      cwd: scratch,
      env: { ...process.env, SOL_CLIO_NATIVE_OUT_OF_TREE: '1', SOL_CLIO_NATIVE_BUNDLE: bundle },
      encoding: 'utf8',
    });
    assert.equal(child.status, 0, `out-of-tree run failed: ${child.stderr}`);
  }
  console.log(JSON.stringify({ node: process.version, evidence }));
} finally {
  await rm(scratch, { recursive: true, force: true });
}
