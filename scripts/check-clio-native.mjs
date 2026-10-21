// Test a compiled Sol/Clio ESM adapter with isolated files and no provider calls.
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const bundle = resolve(process.argv[2] ?? 'target/clio-native/probe.js');
const scratch = await mkdtemp(join(tmpdir(), 'sol-clio-native-'));
try {
  const nativeMapped = async () =>
    process.platform !== 'linux' || /fs-ext/.test(await readFile('/proc/self/maps', 'utf8'));
  const { probe } = await import(pathToFileURL(bundle).href);
  // Clio and Sol load fs-ext-extra-prebuilt on the first lock, not at import.
  if (process.platform === 'linux') {
    assert.equal(await nativeMapped(), false, 'native lock binding loaded at import');
  }
  const evidence = await probe(scratch);
  assert.equal(await nativeMapped(), true, 'first lock did not load the native binding');
  assert.match(evidence, /:status :passed/);
  assert.match(evidence, /:events 2/);
  assert.match(evidence, /:identical-retry true/);
  assert.match(evidence, /:reopen-replay true/);
  assert.match(evidence, /:stream-sequences \[1 2\]/);
  console.log(JSON.stringify({ node: process.version, evidence }));
} finally {
  await rm(scratch, { recursive: true, force: true });
}
