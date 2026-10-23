# AGENTS.md — `@eta-mu/sol`

Read the repository-root `AGENTS.md` first; this file only adds Sol-specific
boundaries. The Clojure House Rules (categories vs. contracts, zero warnings,
namespace layering, construction order, modern `^:async` CLJS) apply here in
full.

## What Sol is

Sol is the eta-mu ClojureScript agent runtime backend — a small Node 22 +
shadow-cljs + Fastify control plane. It exposes a `/health` probe, an
OpenAI-compatible `/v1/*` surface, and an `/api/agent/*` turn/session/run API
over HTTP, plus a `/ws/stream` realtime channel. It runs one agent turn at a
time against Proxx-backed models. It is the trimmed-down, eta-mu-native
successor of the Knoxx backend.

See [`README.md`](README.md) for the namespace map, build targets, route surface,
and configuration.

## Namespace layering

All source is `open-hax.sol.*` under `src/cljs/open_hax/sol/`. Follow the
layer rules from the root `AGENTS.md`:

| Layer | Path | Rule |
|-------|------|------|
| `law.*` | `law/`, plus `shape/app-shapes` route grammar | Malli / contract validators. No I/O. |
| `shape.*` | `shape/` | Pure data morphisms (parse, normalize, persistence shapes). Domain-agnostic. |
| `extern.*` | `extern/` | The only place that touches raw JS/Node/SDK objects (fastify, fetch, json, tools, fs, eta-mu). Nothing above extern handles a raw host object. |
| `domain.*` | `domain/` | Pure decisions over shaped data (agent, contracts, models, realtime, text, time). No I/O. |
| `infra.*` | `infra/` | Effect orchestration: config, HTTP server, lifecycle, agent runner/service/session/run-state, route registration. Composes extern + domain. |
| `runtime.*` | `runtime/state.cljs` | Process-local runtime/config atoms. |

`bootstrap.cljs`, `entrypoint.cljs`, and `plugin.cljs` are the thin top-level
seams; keep startup logic in `bootstrap`/`infra`, not in `entrypoint`.

Build in the fixed construction order: `law → shape → extern → domain → infra`.
Each layer compiles against already-defined lower layers; that ordering *is* the
dependency DAG.

## Routes and macros

- Routes are defined with the `defroute` macro (`macros.cljc`) and registered in
  `infra/routes/app.cljs` via `register-routes!`. The WS route is registered by
  `domain/realtime/register-ws-routes!`.
- `route!` itself is defined in `shape/app-shapes`. Route handlers receive
  `request`/`reply` and a `deps` map; keep handlers thin and push logic into
  `infra/agent/*` services and `domain/*` decisions.
- Macros are registered in the build (`:macros {:open-hax.sol.macros true}`) and
  must be reflected in `.clj-kondo/` config so kondo stays at zero warnings.

## Boundaries

- Node and npm modules (`node:*`, `fastify`, `ws`, `typebox`,
  `@modelcontextprotocol/sdk`) are runtime imports via
  `:keep-as-import`; do not let them leak above `extern/`.
- `shadow-cljs.edn` uses `:deps true`. `deps.edn` consumes Katamorph and
  Clio at immutable Git revisions, not sibling source directories.
  Treat those as read-only upstream dependencies; fix their laws in their
  owning repositories before changing Sol's pins.
- No retrieval/RAG layer lives in Sol; `chat` and `direct` routes run the same
  plain turn. If retrieval is added it belongs in front of the turn (context
  injection or a tool), not threaded through the agent loop as a mode.

## Relationship to Knoxx and the eta-mu runtime

- **Knoxx** (`knoxx.backend.*`, port `8000`) is the full agent workbench. Sol is
  its eta-mu rename and a much smaller subset: no admin/RBAC, memory,
  studio/voice, MCP-HTTP, or proxy surface. Sol's `/api/agent/*` routes are
  wire-compatible with Knoxx's `/api/knoxx/*`.
- **Lineage to clean up**: Sol still reads `KNOXX_*` env vars and ships an
  inherited `knoxx.*` mutation namespace. These are tracked lineage, not new
  dependencies — see the README's KNOXX_* section. Do not introduce new `KNOXX_*`
  names or new `knoxx.*` namespaces; new config should be `SOL_*` and new code
  `open-hax.sol.*`.
- **eta-mu runtime**: Sol runs agent turns on the turn-processor run-loop
  (`eta-mu.turn-processor.infra.loop`) with `eta-mu.extern.openai` streaming,
  consumed at an immutable eta-mu Git revision with `:deps/root` selecting
  `packages/turn-processor` and `packages/eta-mu` in `deps.edn`. The adapter under
  `infra/agent/provider/` wires them into `IAgentProviderAdapter`; sol no
  longer consumes the legacy coding-agent CLI package.

## Proposed persistent review hosting

[The phase-one proposal](docs/design/persistent-review-workers.md) and its
[EDN integration contract](docs/design/persistent-review-workers.edn) are
planning artifacts, not loaded runtime resources or a deployed service.
They refine [Foresight story 330aa63f](https://github.com/open-hax/foresight/pull/122)
without changing that card's Rheos state. Planning settlement and an actual
Rheos-ready admission precede implementation and service activation.

Eta-mu owns review admission, policy and GitHub publication; Sol owns execution;
Knoxx owns the authenticated API/projection; Clio owns immutable
records; Katamorph owns reusable resource shapes; Services owns deployment.
Extend Sol's existing agent service and turn processor. Do not create a second
agent loop, local board parser, local event-envelope law or publisher identity.
Candidate tools receive neither App signing nor deployment credentials.

The bounded [Clio cutover](docs/design/sol-clio-cutover.md) consumes the actual
upstream envelope/catalog/admission API. Filesystem persistence is explicitly
opt-in; it does not qualify durable acknowledgement, old-history migration,
lease/fence behavior or persistent review hosting.

For every pull-request interaction, read
`~/.agents/skills/pr-flow/SKILL.md`. The canonical skill pack owns invitation,
review convergence and merge policy; follow the user’s current all-available
agent instruction. Keep repository instructions as a pointer rather than
copying that policy here.

## Verification

A task is not done while any relevant suite is failing.

```bash
pnpm lint        # clj-kondo, must be zero warnings
pnpm test        # local compiler + completed, nonempty CLJS result guard
pnpm test:guard  # Node guard regressions, including false-success cases
pnpm check:startup # isolated localhost health + SIGTERM, never a daemon
pnpm build:clio-native # test-only ESM transport probe
pnpm check:clio-native # real append/retry/reopen, no provider inference
pnpm build       # shadow-cljs compile server — also do this for runtime changes;
                 # the test build can pass while the server build fails
```

Inspect compiler and test output for failures even when the command exits zero —
a green exit code does not mean a green build.
</content>
