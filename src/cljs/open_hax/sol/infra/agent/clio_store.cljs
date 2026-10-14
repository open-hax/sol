(ns open-hax.sol.infra.agent.clio-store
  "Opt-in Clio filesystem transport. No history migration or durable-ack claim."
  (:require [clio.domain.schema :as schema]
            [clio.extern.js.crypto :as crypto]
            [clio.extern.js.fs :as fs]
            [clio.infra.ledger :as ledger]
            [clio.infra.runtime :as runtime]
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
  (let [schema-preexisting? (fs/exists? schema-directory)
        ledger-created? (atom false)
        schema-created? (atom false)]
    (try
      (if initialize?
        (do
          (when (or (fs/exists? ledger-file) schema-preexisting?)
            (throw (ex-info "Clio initialization requires a fresh storage epoch" {})))
          ;; Claim the schema directory before creating the ledger parent or
          ;; file. If schema-directory is an ancestor of ledger-file,
          ;; ensure-dir! on the ledger parent would create the schema dir
          ;; first; exclusive mkdir would then fail with EEXIST and
          ;; schema-created? would stay false, so rollback would leave an
          ;; orphan directory that blocks the next fresh-epoch check.
          (fs/ensure-dir! (path/dirname schema-directory))
          (exclusive-fs/create-directory! schema-directory #(reset! schema-created? true))
          (fs/ensure-dir! (path/dirname ledger-file))
          ;; Same empty exclusive file as ledger/create-ledger!, but ownership
          ;; is recorded as soon as the exclusive open succeeds, so a failing
          ;; descriptor close still rolls the new ledger back. An exclusive-open
          ;; failure never claims (or deletes) a file another attempt created.
          (exclusive-fs/create-file! ledger-file #(reset! ledger-created? true)))
        (do
          ;; Upstream refuses deleted/misspelled paths instead of inventing history.
          (ledger/read-ledgers [ledger-file])
          ;; Reopening is not initialization, including a missing schema history.
          (when-not schema-preexisting?
            (throw (ex-info "Explicit Clio schema history is missing" {})))))
      (let [runtime (runtime/open schema-directory lifecycle/catalog)]
        (ledger/canonicalize-files (:schema/revisions runtime) [ledger-file])
        {:runtime runtime :ledger-file ledger-file :schema-directory schema-directory})
      (catch :default error
        (when @ledger-created?
          (attempt-cleanup! #(fs/delete-if-exists! ledger-file)))
        (when @schema-created?
          (attempt-cleanup! #(fs/remove-tree! schema-directory)))
        (throw error)))))

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
   names other paths is refused rather than returned with a mismatched store."
  [config store]
  (let [requested (mapv config [:clio/ledger-file :clio/schema-directory])]
    (when (and (some some? requested)
               (not= requested (mapv store [:ledger-file :schema-directory])))
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
