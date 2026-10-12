# Sol dependency and build evidence

Observed 2026-10-03 in the clean standalone fork at
`1276955c86ff46936cd1ce7d81fbc790f7068e1e`. No sibling package, credential,
production service or dependency manifest was changed for these probes.

| Probe | Actual result |
| --- | --- |
| `clojure -P` | Exit 0. Resolved `deps.edn`, including eta-mu `0ed56aa…` subdirectory Git dependencies, Katamorph `305a5e4…` and event-ledger `ada7374…`. No missing immutable Git dependency observed. |
| `clojure -M -m shadow.cljs.devtools.cli compile server` | Exit 0; 181 files, 165 compiled, **0 warnings**, 7.55 seconds. Generated ESM retains Node/npm imports; compile success does not prove runtime startup. |
| `clojure -M -m shadow.cljs.devtools.cli compile test` | Exit 0; 191 files, 190 compiled, **0 warnings**. **Autorun failed** with `Cannot find module '@modelcontextprotocol/sdk/client/index.js'`. Also reported missing `source-map-support`. No passing CLJS test counts. |
| `npm ci --ignore-scripts --no-audit --no-fund` | Exit 1, `EUSAGE`: package/lock mismatch. Locked shadow-cljs `3.4.4` cannot satisfy manifest `^3.4.10`; resolver reported `3.5.4` plus missing buffer/process/readline-sync/shadow-cljs-jar/source-map-support/which dependencies. Nothing was installed successfully. |
| `pnpm lint` | Exit 0. Kondo **0 errors, 0 warnings**; contract-guard OK. Kondo also emits inherited layering **info** diagnostics; they are not proof that layering is clean. |
| `node --test scripts/contract-guard.test.mjs` | Exit 0, 4 passed, 0 failed. Tests the existing guard, not the CLJS runtime. |

Tools were present: Node **24.14.1**, Clojure CLI **1.12.2.1565**, Java,
Babashka, pnpm, clj-kondo. This was **not** a Node 22 runtime qualification.
pnpm warned that the operator npmrc contains an unresolved `NPM_TOKEN`
placeholder; no secret value was printed or supplied.

The lockfile root still identifies `@open-hax/knoxx-backend-cljs` and includes
the older broad Knoxx dependency surface. The package manifest identifies
`@eta-mu/sol`. A reproducible package-manager/lockfile repair belongs in a
separate runtime prerequisite change, with Node 22 clean install, actual CLJS
test counters, server startup and both server/test compilation. Do not update
the immutable Git pins or pull local sibling sources merely to hide this npm
failure. Do not install an unrecorded floating npm graph as build proof.

The existing `scripts/run-shadow-tests-ci.mjs` refuses success when it cannot
parse CLJS result counters. The package `test` alias invokes shadow directly,
so its zero exit status alone is insufficient. This planning PR does not change
the runtime/test command; future qualification must preserve that honest gate.

The proposal's smallest runnable check is:

```bash
bb scripts/check-review-worker-plan.clj
bb scripts/check_review_worker_plan_test.clj
```

It reads EDN, requires the complete immutable Git dependency set from `deps.edn`
and an independently reviewed v1 source-file inventory, compares complete pin
fields, verifies namespace anchors, and ensures the proposal stays disabled.
The CLI regression fixtures prove that omitted/extra dependencies or integration
files, incomplete pins, duplicates and service activation are rejected.
It is only a structural/drift check. Canonical Katamorph resource validation,
Clio replay, Rheos-ready state and hosted MiMo/Kimi execution are still pending.

## Admitted Node 22 runtime prerequisite (2026-10-03)

Canonical Rheos task `146f1b47-c6a7-5a37-996b-a381dd91f6b6` was admitted ready
at 3 points by the parent. This repair closes the standalone npm/test/startup
prerequisite from the planning evidence above; it does not admit hosted workers
or deployment. The parent retains board transition authority.

The manifest records Node **22.20.0** / npm **10.9.3**, retains the existing npm
lock format and pnpm script aliases, and pins the local shadow npm wrapper to
**3.4.11**. The rebuilt lock identifies Sol and contains 165 package entries,
including MCP SDK **1.32.0** under the existing `^1.29.0` manifest range, with
registry integrity records and **zero links/external sibling paths**. No Clojure
Git pin or sibling-source substitute changed. Lock SHA256:
`92b8240163619d3284e2649e29e01da182defe8da30a43fd03b2ab9540a2291b`.

| Node 22 probe | Actual result |
| --- | --- |
| Normal `npm ci --no-audit --no-fund`, repeated | Both exit 0; 165 packages. Manifest/lock remained unchanged by installation. Initial `--package-lock-only` resolution was recorded, then qualification used the frozen graph, including normal lifecycle scripts. |
| First actual `npm test` after install | Exit 1; 125 tests / 405 assertions, 0 failures / 1 error. The guard exposed the inherited `../../scripts/contract-guard.mjs` test path. Corrected to the standalone repository's `scripts/contract-guard.mjs`; no application test was removed or skipped. |
| Clean `npm test` after deleting owned compiler output/cache | Exit 0; **125 tests / 467 assertions**, **0 failures / 0 errors**; 191 files, 190 compiled, **0 warnings**. Uses the installed local npm wrapper rather than the global CLI. |
| Clean `npm run build` | Exit 0; 181 files, 165 compiled, **0 warnings**. |
| `npm run test:guard` | Passing Node regression tests exercise nonempty success and reject crashes/missing counters, zero tests/assertions, conflicting/failing counters, warnings, nonzero exits/signals, missing executable and timeout. Also runs the existing contract-guard tests. |
| `npm run lint` | Exit 0; **0 errors / 0 warnings**, contract-guard OK. Inherited layering INFO diagnostics remain visible. |
| Planning structural checker and CLI tests | Exit 0; 4 unchanged pins / 16 integration files; **8 tests / 15 assertions**, 0 failures / 0 errors. This does not validate a board or a worker resource. |
| Separate `npm ci --omit=dev` production snapshot | Exit 0; 152 packages, identical lock bytes. |
| `node scripts/check-startup.mjs <production-snapshot>/dist/server.js` | Exit 0. Actual Node v22.20.0 server bound `http://127.0.0.1:44817`; GET `/health` returned HTTP 200 and `{"status":"ok","service":"open-hax-sol-cljs","at":"2026-10-03T09:29:48.711Z"}`; SIGTERM exited **0**. |

The startup check supplied an explicit environment without provider/App/deploy
credentials, an empty temporary contract root and isolated workspace/state. It
made only a localhost health request, terminated the temporary process and
removed its scratch state. No production process, service, board status or
review worker was activated.

`.github/workflows/node22-runtime.yml` runs the frozen install, immutable Git
resolution, guard/source/plan checks, actual CLJS tests, server build and separate
production-dependency health/SIGTERM boundary on GitHub. Local results above are
not a claim that remote CI or exact-head review has completed; those must be
confirmed before merge. Detailed local command/exit/log hashes are retained in
`.ημ/node22-prerequisite/runtime-probes.jsonl` and append-only receipts.

### Fresh hosted dependency-access gap

The first GitHub run on `11f19fa1d4519c770d05f833f7b1a1994fd37992`
([37113465917](https://github.com/riatzukiza/sol/actions/runs/37113465917))
passed the frozen npm install, then failed `clojure -Srepro -P` cloning
`open-hax/event-ledger`. Read-only GitHub metadata confirmed that repository is
private and the pinned commit exists; the other declared Git repositories are
public. The fork had no Actions secrets. The operator's pre-existing Git cache
explains local success and does not qualify clean hosted resolution. The later
CI test/build/startup steps were skipped, not passed.

CI now requires parent-provisioned `SOL_DEPENDENCY_TOKEN` with read-only access
to that dependency. It is supplied only to Git resolution, using a temporary
askpass helper and Git's `credential.useHttpPath`; the helper refuses other
URLs and is removed on exit. Application checks do not inherit this credential.
Four public-dummy helper fixtures verify the intended username/password prompts
and rejection of another repository or a host-only prompt. No real credential
was read, printed or supplied by the Sol sidecar. No pin/Clio migration occurred.
Fresh remote qualification remains pending until that credential is supplied.

CodeRabbit's first review found that the zero-warning guard missed singular
`warning`. A regression reproduced exit 0 for that case; the guard now accepts
both warning-count spellings and rejects either when nonzero. All 13 Node guard
tests pass after the correction. This is review settlement, not an approval of
the resulting new head.

### Public dependency correction and scope boundary

On 2026-10-03 the parent made `open-hax/event-ledger` public; a fresh GitHub
API read confirmed `private: false`, `visibility: public`. The private-access
observations above describe the earlier runs and are retained as provenance.
The workflow now removes the obsolete secret/askpass seam and resolves the
unchanged pinned HTTPS dependencies with credential helpers and prompts
disabled. Hosted CI must run the actual test/build/production-startup steps
before this baseline can merge.

The user separately corrected Sol's record authority to Clio. That is not a
rename of `open-hax.event-ledger`: Clio `788cdd3434615a7932b924e68520dbf7f88408c2`
uses content-derived schema references, qualified event-type keywords, UUID
event IDs, explicit stream sequence numbers and causal edges. Its current
append API is an OS-locked EDN file operation, rather than the predecessor's
Mongo handle plus envelope API. The canonical 3-point Node 22 card explicitly
excludes Git-pin migration. PR #2 therefore retains its original four pins;
a separately admitted/reviewed Clio prerequisite follows the qualified
baseline. A passing Node 22 baseline is not evidence that Clio is integrated
or that hosted review workers are ready.


## Local Clio filesystem cutover evidence — 2026-10-04

Base Sol `72fdecca292859d92cba48b686ec6fe9c43a9133`; Clio
`788cdd3434615a7932b924e68520dbf7f88408c2`. The bounded
[cutover contract](sol-clio-cutover.md) records supported configuration and gaps.
Historical qualification above remains evidence of those earlier revisions.

| Local check | Actual result |
| --- | --- |
| Retry/Mongo RED | 127 tests / 469 assertions, two expected failures. |
| Missing-schema reopen RED | 134 / 506, two expected failures; one helper warning subsequently repaired. |
| Equivalent empty-payload RED | 135 / 508, two expected failures. |
| Final guarded CLJS suite | 136 tests / 511 assertions, zero failures/errors/warnings. |
| Lint/contract guard | Zero errors/warnings; inherited informational findings remain visible. |
| Test-runner/contract-guard regressions | 13 passing tests. |
| Planning CLI/checker | Four immutable pins, 18 source files; eight tests / 15 assertions pass. |
| Frozen Node22 npm install | 167 packages; package/dependency manifests unchanged. |
| Actual portable catalog on JVM | Loads successfully; six upstream-composed catalog leaves. |
| Server / native ESM builds | 197 / 95 files; zero warnings. |
| Separate production-only install | 154 packages; authoritative npm lock unchanged. |
| Production ESM real-file probe | Two events, identical lost-ack retry, sequences 1/2, explicit cause and complete reopen/replay. |
| Production localhost startup | Default and Clio-enabled modes both health 200 and clean SIGTERM exit 0. |
| Workflow/diff syntax | actionlint and git diff check pass. |

The final production probes were repeated after the payload/hot-reload fixes.
No provider inference, credential use, daemon change, deployment, commit, push,
PR, review request or Rheos state write occurred. Mongo, power-loss-durable ack,
legacy-history import, worker lease/fence and restart intent remain upstream or
later hosting gaps. GitHub CI and exact-head review have not been qualified.
