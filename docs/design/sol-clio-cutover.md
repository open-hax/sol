# Bounded Sol lifecycle cutover to Clio

This is the first local runtime slice, based on Sol
`72fdecca292859d92cba48b686ec6fe9c43a9133`, consuming immutable Clio
`788cdd3434615a7932b924e68520dbf7f88408c2`. It is not persistent review-worker
hosting or a deployment. GitHub publication and board admission remain with the
parent. Historical receipts and prior build evidence remain unchanged.

## Actual authority and runtime

Clio owns generic envelopes, schema identity, validation, admission and complete
history canonicalization. Sol imports `clio.law.schema/event-schema`,
`clio.domain.schema/validate-event!`, `clio.infra.event/make-event`,
`clio.infra.ledger/append-event!` and `canonicalize-files` directly. No generic
ledger law, UUID/hash law, causal canonicalizer or Rheos implementation is copied.

`law/lifecycle_catalog.cljc` defines only Sol lifecycle data. The six existing
lifecycle names correspond to `:sol.run/started`, `:sol.run/failed`,
`:sol.run/completed`, `:sol.turn/started`, `:sol.turn/failed` and
`:sol.turn/completed`. Each episode is one Clio stream starting at sequence 1.
Run/session/turn/episode IDs, transport attribution, supplied principal binding,
resource revision, conversation and payload facts are carried in `:event/data`.
Actor attribution is not a newly authenticated principal. Axxium and Katamorph
retain authority; review-job data admission is a later upstream prerequisite.

Clio stamps event UUID/time and content-derived schema identity. Every later
stream event names its previous accepted event in `:event/causes`. Sol constructs
once and retains the exact event through an ambiguous acknowledgement; a retry
calls upstream `append-event!` and accepts only `:appended` or `:already-present`.
Stream state advances after acknowledgement. Before a different lifecycle fact
or compensation, any pending write is re-appended and resolved. Concurrent
emission on the same episode is refused, rather than racing its process-local
state. Pending identity does not survive process restart in this slice.

Start persistence errors still prevent execution. Execution errors remain errors.
A successful local execution followed by terminal append failure still returns
its local result and separately reports canonical persistence failure through
`:clio/report-error!`. Such a result does not prove canonical terminal acceptance
or review eligibility.

Clio's native dependency `fs-ext-extra-prebuilt` is pinned to 2.2.9 in the Sol
production npm lock. Actual compiled ESM imports and native locks require the
exact Node 22.20.0/npm 10.9.3 production install qualification below.

## Explicit opt-in filesystem configuration

| Setting | Contract |
| --- | --- |
| `SOL_CLIO_LEDGER_FILE` | Explicit new-epoch ledger file; no default path conversion. |
| `SOL_CLIO_SCHEMA_DIR` | Explicit catalog-history directory for that epoch. |
| `SOL_CLIO_INITIALIZE=true` | First initialization only. Both paths must be new; existing paths are refused. |
| Ordinary reopen | Remove initialization flag. Ledger and schema history must already exist. Complete canonical history is verified before HTTP readiness. |

Example for an operator-provisioned fresh epoch, not a deployment instruction:

```text
SOL_CLIO_LEDGER_FILE=/state/sol/clio-epoch-1/events.edn
SOL_CLIO_SCHEMA_DIR=/state/sol/clio-epoch-1/schemas
SOL_CLIO_INITIALIZE=true
```

After successful initialization, remove `SOL_CLIO_INITIALIZE` before restarting.
Do not point these settings at legacy files. New canonical appends go only to the
explicit Clio path. Neither old ledger bytes nor old schema snapshots are
rewritten or imported. Existing session/run projections keep their current paths.

Without opt-in configuration, turns remain validation-only local execution;
canonical session append/read is unavailable. `:event-ledger-db` is refused as
unsupported Mongo; `:event-ledger-append!` is refused with the new explicit
admission-result contract. A host can supply an opened `:clio/store` or an explicit
`:clio/append!` function which returns actual Clio admission results. Filesystem
paths must be opened/configured before creating episodes; they never silently
fall back to validation-only mode.

Session event append/read delegates to the same Clio transport. Events must
belong to the requested Sol session. Reads canonicalize complete named history
before filtering session facts; malformed lines and missing paths are not skipped.
Mutable session projection operations remain separate. HTTP hot reload retains
the same episode/session transport; changing filesystem roots requires a full
bootstrap. Catalog/schema changes also require a full bootstrap for a new current
schema revision.

## Upstream gaps kept visible

| Capability | Observed gap / proper follow-up |
| --- | --- |
| Mongo persistence | Clio 788cdd has no Mongo adapter/storage protocol. Add transport upstream if required, then consume it; no implicit conversion here. |
| Durable acknowledgement | Upstream locked append calls `appendFileSync` without `fsync`/`fdatasync`. This slice proves native append acceptance, not power-loss durability. The worker contract still requires upstream proof/capability. |
| Legacy continuity | No predecessor bridge is exposed. Arbitrary old IDs, envelope versions and Mongo global sequence cannot be treated as Clio UUIDs/per-stream history. Preserve old bytes; admit migration upstream separately. |
| Lease/fence/restart intent | An inode append lock and process-local pending data are not a worker lease, monotonic fence or restart intent store. Existing worker requirements remain unsatisfied. |
| Incremental replay | This revision canonicalizes complete history. No suffix-only/checkpoint recovery claim is made. |
| Review eligibility | Existing local result/projections are not canonical full-input review evidence. Eta-mu owns admission/publication and upstream review profiles remain required. |

Append admission checks a physical ledger partition. It can accept an event whose
causal completeness is only decidable when complete history is canonicalized.
Sol delegates that distinction to Clio; it does not implement a second admission
kernel. The current adapter uses one explicitly named ledger for its supported
slice. Multi-host allocation/failover is not qualified.

## Verification and limits

`npm test` uses the existing completed, nonempty CLJS test guard. New real-file
coverage exercises exact-event lost-ack retry, contiguous sequence/causes,
reopen/replay, native duplicate admission, changed-data UUID collision, competing
stream slot, session filtering, malformed/missing history, unknown schema roots,
fresh epoch refusal and unchanged legacy/schema bytes. Existing authentication,
resource revision, lifecycle failure and terminal failure cases remain covered.

The test-only compiled ESM target contains the real Sol/Clio adapter. Its probe
writes actual temporary ledger/schema files, deliberately loses an acknowledgement,
retries the identical event, appends a successor and reopens complete history.
No provider is called. Run:

```text
npm run test:guard
npm run lint
bb scripts/check-review-worker-plan.clj
bb scripts/check_review_worker_plan_test.clj
npm test
npm run build
npm run build:clio-native
npm run check:clio-native
npm run check:startup
npm run check:startup -- dist/server.js --clio
```

The hosted Node 22 workflow retains frozen npm install, anonymous public pinned Git
dependency preparation, guard/lint, plan checks, actual tests, server build and
health/SIGTERM checks. Its isolated production stage uses `npm ci --omit=dev`,
compares the authoritative lockfile, then runs both the compiled Clio ESM probe
and default/opt-in server startup. Local native evidence is recorded separately;
no GitHub CI/review/deployment success is inferred from it.
