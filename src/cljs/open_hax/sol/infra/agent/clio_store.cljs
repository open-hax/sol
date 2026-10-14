(ns open-hax.sol.infra.agent.clio-store
  "Opt-in Clio filesystem transport. No history migration or durable-ack claim."
  (:require [clio.domain.schema :as schema]
            [clio.extern.js.crypto :as crypto]
            [clio.extern.js.fs :as fs]
            [clio.infra.ledger :as ledger]
            [clio.infra.runtime :as runtime]
            [clio.infra.schema-store :as schema-store]
            [clojure.string :as str]
            [open-hax.sol.domain.node.path :as path]
            [open-hax.sol.extern.exclusive-fs :as exclusive-fs]
            [open-hax.sol.law.lifecycle-catalog :as lifecycle]))

(defonce local-revision
  (delay (schema/materialize crypto/sha256 lifecycle/catalog)))

(defn current-revision
  "No-appender mode still uses upstream schema identity and validation."
  [store]
  (if store (:schema/current (:runtime store)) @local-revision))

(defn- attempt-cleanup!
  "Best-effort rollback; never mask the initialization error being rethrown."
  [cleanup!]
  (try
    (cleanup!)
    (catch :default _ nil)))

(defn- canonical-path
  "Absolute path with existing ancestors realpath'd and missing suffixes
   appended. Symlink aliases that meet the same physical location compare
   equal, so nest checks see the layout the runtime will actually use."
  [p]
  (let [resolved (path/resolve p)]
    (loop [cur resolved
           suffix ()]
      (if (fs/exists? cur)
        (apply path/join (exclusive-fs/realpath cur) suffix)
        (let [parent (path/dirname cur)]
          (if (= parent cur)
            resolved
            (recur parent (cons (path/basename cur) suffix))))))))

(defn- path-beneath?
  "True when `descendant` resolves strictly under `ancestor`.
   Existing parents are canonicalized (realpath) before the check so a
   symlink alias that reaches a nested layout is refused. An absolute
   relative (Windows cross-drive) is not a descendant. Parent-escape is a
   path-segment check: a component named exactly `..` escapes; a sibling
   segment that merely starts with `..` (e.g. `..d`) remains a descendant."
  [ancestor descendant]
  (let [a (canonical-path ancestor)
        d (canonical-path descendant)
        rel (path/relative a d)]
    (and (not= a d)
         (not (str/blank? rel))
         (not (path/absolute? rel))
         (not (some #(= ".." %) (str/split rel #"[\\/]"))))))

(defn- missing-schema-history! []
  (throw (ex-info "Explicit Clio schema history is missing" {})))

(defn- has-root? [revisions root]
  (boolean (some #(= root (:schema/root %)) revisions)))

(defn- persist-evolved-revision!
  "Add the current revision to existing schema history without creating any
   directory. Same snapshot bytes as Clio's schema-store/ensure-revision!, but
   written through an exclusive create inside the already-existing directory:
   a directory that vanished fails ENOENT instead of being recreated. A
   concurrent opener publishing the same root first is accepted; the reload
   that follows verifies every snapshot against its recorded root."
  [schema-directory revision]
  (let [root (:schema/root revision)
        target (schema-store/revision-path schema-directory root)
        staging (str target ".tmp-" (random-uuid))
        staged? (atom false)
        snapshot {:schema/root root :schema/catalog (:schema/catalog revision)}]
    (try
      (exclusive-fs/write-new-file! staging (str (pr-str snapshot) "\n") #(reset! staged? true))
      (try
        (exclusive-fs/link-new! staging target)
        (catch :default error
          (when-not (= "EEXIST" (.-code error)) (throw error))))
      (finally
        (when @staged? (attempt-cleanup! #(fs/delete-if-exists! staging)))))))

(defn- reopen-runtime
  "Ordinary reopen loads schema history through non-creating operations only.
   runtime/open (and ensure-revision!) recursively create the directory, so a
   directory removed after validation would be silently re-materialized with
   only the current revision; this never calls them. Missing or empty history
   refuses. A source catalog edit still adds its revision, written only into
   the existing directory."
  [schema-directory]
  (let [current @local-revision
        root (:schema/root current)
        persisted (schema-store/load-revisions schema-directory)]
    (when (empty? persisted) (missing-schema-history!))
    (let [revisions (if (has-root? persisted root)
                      persisted
                      (do (persist-evolved-revision! schema-directory current)
                          (schema-store/load-revisions schema-directory)))]
      (when-not (has-root? revisions root) (missing-schema-history!))
      {:schema/directory schema-directory
       :schema/current current
       :schema/revisions revisions})))

(defn- initialize!
  "Create a fresh storage epoch with atomic ledger ownership. The schema
   directory is claimed by exclusive mkdir. The ledger is created under a
   private staging name beside the target and published with an exclusive
   hard link only after runtime/open and canonicalization succeed. Until that
   publish nothing exists at the ledger path, so no concurrent reopen can adopt
   or append to a ledger this attempt might roll back; after it, the epoch is
   committed and nothing is unlinked except the staging alias. Publishing
   fails EEXIST if another attempt won the ledger path, and rollback removes
   only this attempt's staging file and owned schema artifacts. The schema
   directory is only adoptable by a reopen that names a ledger this attempt
   never published, so rolling it back cannot strand accepted history."
  [ledger-file schema-directory]
  (when (or (fs/exists? ledger-file) (fs/exists? schema-directory))
    (throw (ex-info "Clio initialization requires a fresh storage epoch" {})))
  ;; Nested ledger/schema layouts are refused before creating anything:
  ;; schema under the ledger path leaves a directory at the ledger on
  ;; failure; ledger under the schema path is treated as a schema
  ;; snapshot by runtime and makes the store unusable.
  (when (or (path-beneath? ledger-file schema-directory)
            (path-beneath? schema-directory ledger-file)
            (= (canonical-path ledger-file) (canonical-path schema-directory)))
    (throw (ex-info
            "Clio ledger file and schema directory must not nest"
            {:ledger-file ledger-file :schema-directory schema-directory})))
  (let [staging (str ledger-file ".init-" (random-uuid))
        staging-created? (atom false)
        schema-created? (atom false)]
    (try
      ;; Claim the schema directory before creating the ledger parent or
      ;; staging file so exclusive mkdir owns the directory even when a later
      ;; step fails.
      (fs/ensure-dir! (path/dirname schema-directory))
      (exclusive-fs/create-directory! schema-directory #(reset! schema-created? true))
      (fs/ensure-dir! (path/dirname ledger-file))
      ;; Ownership is recorded as soon as the exclusive open succeeds, so a
      ;; failing descriptor close still rolls the staging file back.
      (exclusive-fs/create-file! staging #(reset! staging-created? true))
      (let [runtime (runtime/open schema-directory lifecycle/catalog)
            _ (ledger/canonicalize-files (:schema/revisions runtime) [staging])]
        (exclusive-fs/link-new! staging ledger-file)
        ;; Committed: the ledger path now names this epoch. Only the private
        ;; staging alias remains to drop; its failure cannot undo the commit.
        (reset! schema-created? false)
        (attempt-cleanup! #(fs/delete-if-exists! staging))
        (reset! staging-created? false)
        {:runtime runtime :ledger-file ledger-file :schema-directory schema-directory})
      (catch :default error
        (when @staging-created?
          (attempt-cleanup! #(fs/delete-if-exists! staging)))
        (when @schema-created?
          (attempt-cleanup!
           #(exclusive-fs/rollback-owned-directory!
             schema-directory
             [(schema-store/revision-path schema-directory (:schema/root @local-revision))])))
        (throw error)))))

(defn- reopen!
  [ledger-file schema-directory]
  ;; Upstream refuses deleted/misspelled paths instead of inventing history.
  (ledger/read-ledgers [ledger-file])
  (let [runtime (reopen-runtime schema-directory)]
    (ledger/canonicalize-files (:schema/revisions runtime) [ledger-file])
    {:runtime runtime :ledger-file ledger-file :schema-directory schema-directory}))

(defn open!
  "Open explicitly named complete Clio history. Initialization requires new
   ledger/schema paths and is separate from ordinary append/reopen. A failed
   initialization removes only the artifacts this attempt created, so a retry
   still sees a fresh storage epoch."
  [{:keys [ledger-file schema-directory initialize?]}]
  (doseq [[label value] [[:ledger-file ledger-file]
                       [:schema-directory schema-directory]]]
    (when (or (not (string? value)) (str/blank? value))
      (throw (ex-info "Explicit Clio filesystem paths required" {:path/key label}))))
  (if initialize?
    (initialize! ledger-file schema-directory)
    (reopen! ledger-file schema-directory)))

(defn append!
  "Delegate exact-event admission to Clio; preserve constructed data on retries."
  [store event]
  (when-not store
    (throw (ex-info "Clio filesystem store not configured" {})))
  (let [runtime (runtime/refresh (:runtime store))]
    (ledger/append-event! (:schema/revisions runtime) (:ledger-file store) event)))

(defn events
  "Read validated, complete canonical history, never a forgiving local parser."
  [store]
  (when-not store
    (throw (ex-info "Clio filesystem store not configured" {})))
  (let [runtime (runtime/refresh (:runtime store))]
    (:canonical/events
     (ledger/canonicalize-files (:schema/revisions runtime) [(:ledger-file store)]))))

(defn require-supported-config!
  [config]
  (when (:event-ledger-db config)
    (throw (ex-info "Mongo is not supported by this Clio filesystem adapter" {})))
  (when (:event-ledger-append! config)
    (throw (ex-info "Use explicit :clio/append! with Clio admission results" {})))
  config)

(defn configure!
  "Wire the opt-in filesystem store once during host configuration."
  [config]
  (require-supported-config! config)
  (if (or (:clio/ledger-file config) (:clio/schema-directory config)
          (:clio/initialize? config))
    (assoc config :clio/store
           (open! {:ledger-file (:clio/ledger-file config)
                   :schema-directory (:clio/schema-directory config)
                   :initialize? (:clio/initialize? config)}))
    config))

(defn- require-store-paths!
  "An open store keeps writing to the paths it was opened with; a config that
   names other paths is refused rather than returned with a mismatched store.
   Pre-schema-directory store maps omit :schema-directory; recover it from the
   runtime when present. A requested schema path that cannot be verified
   against the open store requires a full restart."
  [config store]
  (let [requested-ledger (:clio/ledger-file config)
        requested-schema (:clio/schema-directory config)
        store-ledger (:ledger-file store)
        store-schema (or (:schema-directory store)
                         (:schema/directory (:runtime store)))]
    (when (and (or (some? requested-ledger) (some? requested-schema))
               (or (not= requested-ledger store-ledger)
                   (and (some? requested-schema)
                        (not= requested-schema store-schema))))
      (throw (ex-info "Changing Clio filesystem paths requires a full restart" {})))))

(defn ensure-configured!
  "Open the opt-in store for an exported startup seam (start!, solPlugin) that
   did not come through bootstrap!. An already-open store is kept only while
   the config names the same filesystem paths."
  [config]
  (if-let [store (:clio/store config)]
    (do (require-supported-config! config)
        (require-store-paths! config store)
        config)
    (configure! config)))

(defn reconfigure!
  "Keep episode and session transports identical across HTTP hot reload.
   Changing filesystem roots requires a full host bootstrap."
  [previous next-config]
  (require-supported-config! next-config)
  (when-not (= (mapv previous [:clio/ledger-file :clio/schema-directory])
               (mapv next-config [:clio/ledger-file :clio/schema-directory]))
    (throw (ex-info "Changing Clio filesystem paths requires a full restart" {})))
  (assoc next-config :clio/store (:clio/store previous)))
