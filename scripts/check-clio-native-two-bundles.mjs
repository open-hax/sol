// Two copies of a release Sol bundle imported into one host: each copy must
// anchor Clio's native-lock loader at its own module file, not at whichever
// copy was imported first. Release (:simple) ESM output keeps every bundle's
// code module-scoped, so only the shared module-URL registry could leak.
import assert from 'node:assert/strict';
import { copyFile, mkdir, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const bundle = resolve(process.argv[2] ?? 'target/clio-native-release/probe.js');
// The copies stay inside Sol's tree so both resolve Sol's node_modules.
const root = resolve('target/clio-native-two-bundles');
const scratch = await mkdtemp(join(tmpdir(), 'sol-clio-two-bundles-'));
try {
  await rm(root, { recursive: true, force: true });
  const copies = [];
  for (const name of ['first', 'second']) {
    await mkdir(join(root, name), { recursive: true });
    const copy = join(root, name, 'probe.js');
    await copyFile(bundle, copy);
    copies.push(copy);
  }
  // Import both before probing either, as a host embedding two Sol bundles.
  const modules = [];
  for (const copy of copies) modules.push(await import(pathToFileURL(copy).href));
  const results = {};
  for (const [i, copy] of copies.entries()) {
    const evidence = await modules[i].probe(await mkdtemp(join(scratch, 'store-')), copy);
    assert.match(evidence, /:status :passed/);
    results[copy] = /:module-anchor true/.test(evidence);
  }
  assert.deepEqual(results, Object.fromEntries(copies.map((copy) => [copy, true])),
    'each imported Sol bundle must anchor at its own module file');
  console.log(JSON.stringify({ node: process.version, bundles: copies.length, anchors: results }));
} finally {
  await rm(scratch, { recursive: true, force: true });
  await rm(root, { recursive: true, force: true });
}
