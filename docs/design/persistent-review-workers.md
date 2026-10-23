# Persistent MiMo and Kimi review workers: phase one

Status: proposed, not activated. Source first inspected at Sol
`1276955c86ff46936cd1ce7d81fbc790f7068e1e`. The integration inventory in
`persistent-review-workers.edn` names later files (for example
`infra/agent/clio_store.cljs`), so it is validated against the current tree by
`scripts/check-review-worker-plan.clj`, not against a single pinned revision.
Parent: [Foresight PR #122](https://github.com/open-hax/foresight/pull/122),
[story 330aa63f-f697-5bd6-9fc0-3f19fbea2be4](https://github.com/open-hax/foresight/blob/plan/fork-dev-origins/docs/agile/kanban/330aa63f-f697-5bd6-9fc0-3f19fbea2be4.md).

The smallest first slice accepts an eta-mu-authorized dispatch from Actions,
executes an immutable review in Sol, and returns durable evidence for eta-mu to
publish. Ending an Actions waiter must not end an admitted job. Direct GitHub
webhook admission and an interactive GitHub App are later slices.

## Ownership and acceptance boundary

Eta-mu verifies the delivery, principal, repository permission, exact revisions,
manifest and requested reviewer. It owns eligibility and publication policy.
Sol consumes that accepted manifest; it neither interprets webhook events nor
decides whether the PR may merge. Axxium supplies identity bindings. Knoxx owns
the authenticated dispatch/status/findings/cancel/retry API and projection.
Clio owns immutable envelopes, append acceptance and replay;
Katamorph owns reusable job, finding and completion shapes. Services owns the
isolated Node 22 service slot, persistent storage, ingress and rollback.

The [EDN proposal](persistent-review-workers.edn) describes required interfaces
and gaps. It is not a Katamorph schema or another executable workflow engine.
The bounded [filesystem cutover](sol-clio-cutover.md) pins actual Clio source
and delegates generic schema/admission/replay upstream. It preserves old history
without importing it and does not qualify persistent worker hosting. The original
predecessor-to-Clio change requires actual compatibility evidence, not a rename in
this proposal.

## What the source actually provides

| Existing file / namespace | Reuse and required work |
| --- | --- |
| `infra/agent/service.cljs`, `open-hax.sol.infra.agent.service` | Existing `IAgentService` facade. Add a review-job execution delegate around this seam; retain the existing turn loop. Its default queue launches a promise, not a durable scheduler. |
| `infra/agent/runner.cljs`, `open-hax.sol.infra.agent.runner` | Writes queued run state before a background turn and tracks promises in `active-runs*`. Add admitted-job identity/attempt correlation and await canonical acceptance before 202. The atom is process-local, not restart proof. |
| `infra/agent/provider/turn_processor.cljs`, `open-hax.sol.infra.agent.provider.turn-processor` | Existing `IAgentProviderAdapter`, `loop/run-loop`, `openai/stream-chat`, tool registry. Bind the exact requested model and restricted tools; reject unavailable models rather than using the current general fallback behavior. |
| `infra/agent/turn_session.cljs`, `open-hax.sol.infra.agent.turn-session` | Existing per-turn `AbortController`, queues and `abort!`. Wire authorized cancellation to this protocol and prove provider/tool settlement; do not create a second loop. Queues/controllers live in an atom and are not recoverable after process death. |
| `infra/agent/run_state.cljs`, `open-hax.sol.infra.agent.run-state` | Run projections and local event reads. Keep as disposable views. Current append reads and rewrites the whole file, ignores malformed lines on read, and is not an atomic durable journal. `list-active-runs` in the persistence protocol returns `[]`; `run-list-active` is a separate local scan. |
| `infra/agent/session_store.cljs`, `open-hax.sol.infra.agent.session-store` | Session projections/run IDs. Several persistence methods are no-ops or partial projections, not the full advertised run contract. Do not infer restart/cancel guarantees from the protocol docstring. |
| `infra/agent/episode_ledger.cljs`, `open-hax.sol.infra.agent.episode-ledger` | Clio construction/admission via explicit `:clio/store` or `:clio/append!`. Exact pending data survives process-local retries; Mongo is refused. No-appender mode validates only. Durable acknowledgement and restart intent retention remain required for hosting. |
| `infra/agent/clio_store.cljs`, `open-hax.sol.infra.agent.clio-store` | Explicit filesystem epoch, upstream schema snapshots, native append admission and complete replay. No implicit Mongo conversion, fsync acknowledgement, legacy bridge or lease. |
| `law/lifecycle_catalog.cljc`, `open-hax.sol.law.lifecycle-catalog` | Sol lifecycle data composed with upstream `event-schema`; principal/resource authority remains upstream. Review-job profiles still require admission. |
| `infra/agent/episode_turn.cljs`, `open-hax.sol.infra.agent.episode-turn` and `shape/episode_event.cljs` | Existing canonical lifecycle and principal/resource correlation. Extend through owner-admitted profiles. Today a terminal append failure is reported separately while a successful local result is returned; review eligibility must remain blocked until canonical completion is accepted. |
| `law/contract_kinds.cljs` and `domain/contracts/loader.cljs` | Delegates validation to `katamorph.schema`, but unknown kinds fall back to the open agent schema. Admit new kinds upstream explicitly; never install this proposal as a runtime resource through that fallback. |
| `bootstrap.cljs`, `infra/config.cljs`, `infra/graceful_shutdown.cljs` | Store roots currently follow process cwd. Add proposed `SOL_STATE_DIR` and recovery-before-readiness in a later implementation. Shutdown currently closes HTTP/realtime and exits; add fenced admission stop, attempt drain/cancellation and durable interruption evidence. |
| `infra/routes/app.cljs`, `infra/agent/runtime.cljs` | Existing generic run reads, but agent auth context is a no-op. Abort route requests a live control that chooses follow-up/steering and marks a session aborted; it does not call the turn's `abort!`. Keep these routes private; review endpoints need authenticated Knoxx/Axxium capability enforcement and tested cancellation. |

Paths above are relative to `src/cljs/open_hax/sol/`. Existing layering violations
are visible as kondo info diagnostics; do not reproduce them in new work. New
pure decisions should be portable `.cljc`; raw Node objects belong in `extern`,
coordination in `infra`, and canonical law changes in their owning repositories.

## Dispatch and execution

The accepted input binds repository ID, PR number, base/head full SHA, sorted
changed paths, contract revisions, manifest digest, requested reviewer/model,
prompt revision, tool profile and policy revision. Contract revision includes
both source revision and resource-content digest. The server loads the manifest
by digest and re-verifies fetched Git objects before execution. Branch names are
diagnostic labels; they cannot substitute for a SHA.

Job identity is the canonical digest of those immutable inputs. A duplicate
dispatch with the same digest returns the same job; an existing job ID paired
with different content is rejected. A caller retry after a lost 202 response
does not create another attempt. A new head, model or prompt creates a new job.
IDs are opaque, validated path segments; never use repository or caller strings
directly as filesystem paths.

Phase one starts with at most two model executions, one MiMo and one Kimi. Both
are registered independently. The accepted model route is explicit and pinned
in the input; the general provider adapter's fallback cannot choose another
reviewer. Quota, auth and network failures retain their own observed reason.
No fallback model, billing overage or unbounded retry is enabled. A provider
reset time is retained when supplied; a retry requires an authorized decision
and the declared attempt budget (initially two), not another Actions rerun.

The existing Sol turn-processor executes the reviewer. A candidate checkout and
tool executor live in a per-attempt sandbox process with an allowlisted
environment, bounded egress, separate UID/filesystem and no App signing,
deployment or publisher credentials. The trusted provider broker keeps its
scoped Proxx credential outside candidate tools and exposes only the requested
model capability. An env variable in a shared tool process is not isolation.
Sol's current global tool registry is a seam to constrain, not proof of this
boundary. Services must supply and validate the sandbox before any real job.

## Persistence, cancellation and restart

Require a configured Clio-compatible canonical appender before admitting a job.
Persist acceptance before returning 202; persist attempt-start before invoking
the model. A service lease plus monotonically increasing fence identifies the
one permitted writer for each job. Initial topology is one supervisor/slot;
multi-host failover is deferred, and unfenced concurrent supervisors are refused.
State roots must be a Services-mounted persistent volume, not `/tmp` or a
checkout's accidental cwd.

Immutable facts record acceptance, attempt start, cancellation request,
interruption, model outcome, artifacts and publication delivery acknowledgements.
Run/session JSON or EDN views are rebuildable projections. Artifact bytes are
written, synced and content-hashed before a terminal record refers to them.
The canonical appender must provide durable acknowledgement and idempotent
event IDs; replay queries must expose accepted events. These are upstream
Clio capabilities to verify, not local envelope semantics to copy.

Cancellation first records authorized intent. Queued attempts never start;
running attempts call existing `shape.agent/abort!`, then wait for provider and
tool processes to settle or terminate the sandbox after the configured grace.
Terminal `cancelled` requires that acknowledgement and fence. A late result is
retained as evidence, but cannot override cancellation or become an eligible
review. Head supersession uses the same cancellation path and marks publication
ineligible. Client disconnection and Actions timeout never imply cancellation.

On restart, replay accepted job facts and verify stored artifact digests before
readiness. Queued accepted jobs remain queued. Expired running attempts become
`interrupted`; do not claim that LLM token streaming resumed. A distinct new
attempt may restart from immutable inputs only when its authorization, quota
reset and budget permit. Already accepted terminal evidence is reused; its
pending result delivery is reconciled without rerunning inference. A corrupted
record, unavailable appender or ambiguous fence blocks readiness and publication.

## Completion and the result outbox

Model execution completion, evidence eligibility and publication delivery are
separate facts. An answer string, HTTP 200, compiled build or websocket event
cannot substitute for exact-head completed coverage. A terminal result binds
actual requested/executed model, input digest, attempt/fence, source SHA,
findings, coverage, artifact digests, outcome, reason and completion time.
Partial coverage, cancellation, interrupted/quota/auth failures and missing
canonical terminal evidence never become an approval.

Persist a result-delivery intent to the same canonical ledger as part of terminal
acceptance, using one compound owner-admitted event rather than a dual write to
another queue. The Sol outbox is a replayed view of those intents. It sends the
same content-addressed result and idempotency key to eta-mu until an authorized
receipt is durably recorded; network timeout leaves it pending. Eta-mu owns its
separate GitHub publication outbox and checks current PR head, trusted reviewer,
coverage, deterministic checks and settled findings immediately before publishing.
Delivery is at least once with reconciliation; no exactly-once GitHub claim.

Invite CodeRabbit, Codex, MiMo and Kimi. One trusted exact-head approval OR
explicit completed passing verdict with verified coverage satisfies model
quorum. A skip, acknowledgement, quota error or stale verdict does not count.
This does not make the four required deterministic evidence roles in Foresight's
existing composition optional. Sol reports evidence; eta-mu applies the policy.

## Implementation order and qualification fixtures

1. Settle this planning PR and root PR #122; use Rheos to admit the parent story
   to ready. Restore the declared npm lockfile/toolchain so real Sol tests run.
2. Admit job/completion/cancel/outbox profiles and durable idempotent append/replay
   requirements in Katamorph and Clio. Pin accepted compatible revisions in Sol.
3. Add laws/shapes before adapters, with red fixtures for duplicate/mismatched
   dispatch, unsafe revision, unauthorized cancellation, stale fence, partial
   coverage, quota reset and late result after cancellation.
4. Extend the existing service/provider/store/bootstrap seams. Prove crash
   recovery around every accept/start/terminal/delivery acknowledgement boundary,
   including lost responses and terminal append failure. Repeat a delivery without
   duplicate inference or publication. Fail corrupt/truncated input visibly.
5. Review Services' persistent mount, non-root sandbox, fixed source/image,
   ingress, readiness and rollback; Knoxx's anonymous rejection and scoped
   status/findings/cancel/retry API; eta-mu's stale-head publication rejection.
6. Only then prove real MiMo and Kimi runs on the hosted slot, terminate the
   Actions waiter and restart the worker. Record exact source/artifact/provider
   evidence; a document or a fake provider fixture is not deployment proof.

The canonical `~/.agents/skills/pr-flow/SKILL.md` owns reviewer availability,
review convergence and merge policy under the user’s current instruction.
All operational board state remains in Rheos; this proposal does not write
card state, transition events, credentials or server settings.
