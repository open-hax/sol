(ns open-hax.sol.infra.agent.clio-store
  "Opt-in Clio filesystem transport. No history migration or durable-ack claim."
  (:require [clio.domain.schema :as schema]
            [clio.extern.js.crypto :as crypto]
            [clio.extern.js.fs :as fs]
            [clio.infra.ledger :as ledger]
            [clio.infra.runtime :as runtime]
            [clojure.string :as str]
            [open-hax.sol.domain.node.path :as path]
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
        ledger-created? (atom false)]
    (try
      (if initialize?
        (do
          (when (or (fs/exists? ledger-file) schema-preexisting?)
            (throw (ex-info "Clio initialization requires a fresh storage epoch" {})))
          (fs/ensure-dir! (path/dirname ledger-file))
          (ledger/create-ledger! ledger-file)
          (reset! ledger-created? true))
        (do
          ;; Upstream refuses deleted/misspelled paths instead of inventing history.
          (ledger/read-ledgers [ledger-file])
          ;; Reopening is not initialization, including a missing schema history.
          (when-not schema-preexisting?
            (throw (ex-info "Explicit Clio schema history is missing" {})))))
      (let [runtime (runtime/open schema-directory lifecycle/catalog)]
        (ledger/canonicalize-files (:schema/revisions runtime) [ledger-file])
        {:runtime runtime :ledger-file ledger-file})
      (catch :default error
        (when @ledger-created?
          (attempt-cleanup! #(fs/delete-if-exists! ledger-file))
          (when-not schema-preexisting?
            (attempt-cleanup! #(fs/remove-tree! schema-directory))))
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

(defn ensure-configured!
  "Open the opt-in store for an exported startup seam (start!, solPlugin) that
   did not come through bootstrap!. An already-open store is kept as is."
  [config]
  (if (:clio/store config)
    (require-supported-config! config)
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
