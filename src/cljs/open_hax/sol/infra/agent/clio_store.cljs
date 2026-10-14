(ns open-hax.sol.infra.agent.clio-store
  "Opt-in Clio filesystem transport. No history migration or durable-ack claim."
  (:require [cljs.reader :as reader]
            [clio.domain.schema :as schema]
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


(def ^:private init-marker-name ".sol-clio-init")

(defn- init-marker-path
  [schema-directory]
  (path/join schema-directory init-marker-name))

(defn- write-init-marker!
  "Durable record that an initialization publish is in flight. Survives
   process death between the ledger link and the schema revision link."
  [schema-directory marker]
  (fs/write-text! (init-marker-path schema-directory) (str (pr-str marker) "\n")))

(defn- clear-init-marker!
  [schema-directory]
  (attempt-cleanup! #(fs/delete-if-exists! (init-marker-path schema-directory))))

(defn- read-init-marker
  "Return the pending-init map when present and well-formed; otherwise nil."
  [schema-directory]
  (let [marker (init-marker-path schema-directory)]
    (when (fs/exists? marker)
      (try
        (let [data (reader/read-string (fs/read-text marker))]
          (when (and (map? data) (true? (:sol.clio/init-pending data)))
            data))
        (catch :default _ nil)))))

(defn- schema-edn-revisions
  "Revision snapshot basenames (exclude the init marker)."
  [schema-directory]
  (filterv #(str/ends-with? % ".edn") (fs/list-files schema-directory)))

(defn- rollback-incomplete-init!
  "Release an incomplete epoch identified by its marker. Safe only while the
   schema directory holds no revision snapshots: no reopen can have adopted
   it. Deletes the published ledger named by the marker, clears staging
   leftovers, removes the marker, and rmdirs the empty schema directory."
  [{:keys [ledger-file schema-directory staging-ledger staging-schema]}]
  (when (and schema-directory (fs/exists? schema-directory)
             (empty? (schema-edn-revisions schema-directory)))
    (when ledger-file
      (attempt-cleanup! #(fs/delete-if-exists! ledger-file)))
    (when staging-ledger
      (attempt-cleanup! #(fs/delete-if-exists! staging-ledger)))
    (when staging-schema
      (attempt-cleanup! #(fs/remove-tree! staging-schema)))
    (clear-init-marker! schema-directory)
    (attempt-cleanup! #(exclusive-fs/remove-directory-if-empty! schema-directory))))

(defn- resume-incomplete-init!
  "Finish a publish interrupted after the ledger link: link the staged
   revision into the claimed directory (the original commit point), clear
   the marker, and drop private staging. Returns true when resumed."
  [{:keys [schema-directory staging-schema] root :schema/root :as marker}]
  (let [staging-revision (when (and staging-schema root)
                           (schema-store/revision-path staging-schema root))
        target (when root (schema-store/revision-path schema-directory root))]
    (when (and staging-revision target (fs/exists? staging-revision)
               (empty? (schema-edn-revisions schema-directory)))
      (try
        (exclusive-fs/link-new! staging-revision target)
        (catch :default error
          (when-not (= :already-exists (:exclusive-fs/error (ex-data error)))
            (throw error))))
      (clear-init-marker! schema-directory)
      (when-let [staging-ledger (:staging-ledger marker)]
        (attempt-cleanup! #(fs/delete-if-exists! staging-ledger)))
      (attempt-cleanup! #(fs/remove-tree! staging-schema))
      true)))

(defn- recover-incomplete-initialization!
  "If a prior attempt left a durable init marker, resume when staging still
   holds the revision; otherwise roll the incomplete epoch back so initialize
   or reopen can proceed. A marker with an already-published revision is
   treated as committed and only clears leftovers."
  [ledger-file schema-directory]
  (when-let [marker (and (fs/exists? schema-directory) (read-init-marker schema-directory))]
    (cond
      ;; Marker paths must name this open request's locations.
      (not (and (= ledger-file (:ledger-file marker))
                (= schema-directory (:schema-directory marker))))
      nil

      (seq (schema-edn-revisions schema-directory))
      (do (clear-init-marker! schema-directory)
          (when-let [staging-ledger (:staging-ledger marker)]
            (attempt-cleanup! #(fs/delete-if-exists! staging-ledger)))
          (when-let [staging-schema (:staging-schema marker)]
            (attempt-cleanup! #(fs/remove-tree! staging-schema))))

      (resume-incomplete-init! marker)
      nil

      :else
      (rollback-incomplete-init! marker))))

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
      ;; write-new-file! completes short writes; the reload after publishing
      ;; verifies each snapshot hashes to its recorded root.
      (try
        (exclusive-fs/link-new! staging target)
        (catch :default error
          (when-not (= :already-exists (:exclusive-fs/error (ex-data error)))
            (throw error))))
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
  "Create a fresh storage epoch whose artifacts stay private until commit.
   The schema directory is claimed by exclusive mkdir and stays empty, so no
   reopen (of this or any other ledger) can adopt it: empty history refuses.
   runtime/open materializes the revision into a private staging directory
   and the ledger is created under a private staging name; canonicalization
   runs against both. A durable `.sol-clio-init` marker is written before
   publish so process death between the two links is recoverable: the next
   open resumes from staging or rolls the incomplete epoch back. Publishing
   is two exclusive hard links: first the ledger (already-exists if another
   attempt won the path), then the revision into the claimed directory, which
   is the commit point. Before that second link the claimed directory holds
   no revision, so no process can have opened the published ledger or relied
   on the directory, and rollback may release them: it unlinks only staging
   names and a ledger this attempt published, and removes the claimed
   directory only with non-recursive rmdir, which fails (and keeps it) once
   anything else lives there."
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
  (let [attempt (random-uuid)
        staging-ledger (str ledger-file ".init-" attempt)
        staging-schema (str schema-directory ".init-" attempt)
        staging-ledger? (atom false)
        staging-schema? (atom false)
        schema-claimed? (atom false)
        ledger-published? (atom false)]
    (try
      ;; Claim the schema directory before creating the ledger parent or
      ;; staging file so exclusive mkdir owns the directory even when a later
      ;; step fails.
      (fs/ensure-dir! (path/dirname schema-directory))
      (exclusive-fs/create-directory! schema-directory #(reset! schema-claimed? true))
      (exclusive-fs/create-directory! staging-schema #(reset! staging-schema? true))
      (fs/ensure-dir! (path/dirname ledger-file))
      ;; Ownership is recorded as soon as the exclusive open succeeds, so a
      ;; failing descriptor close still rolls the staging file back.
      (exclusive-fs/create-file! staging-ledger #(reset! staging-ledger? true))
      (let [staged (runtime/open staging-schema lifecycle/catalog)
            root (:schema/root (:schema/current staged))
            _ (ledger/canonicalize-files (:schema/revisions staged) [staging-ledger])
            marker {:sol.clio/init-pending true
                    :ledger-file ledger-file
                    :schema-directory schema-directory
                    :staging-ledger staging-ledger
                    :staging-schema staging-schema
                    :schema/root root}]
        ;; Marker before any publish: process death after the ledger link
        ;; leaves enough to resume or roll back on the next open.
        (write-init-marker! schema-directory marker)
        (exclusive-fs/link-new! staging-ledger ledger-file)
        (reset! ledger-published? true)
        (exclusive-fs/link-new! (schema-store/revision-path staging-schema root)
                                (schema-store/revision-path schema-directory root))
        ;; Committed. Clear the marker, then drop private staging aliases;
        ;; their failure cannot undo the commit.
        (clear-init-marker! schema-directory)
        (reset! schema-claimed? false)
        (reset! ledger-published? false)
        (attempt-cleanup! #(fs/delete-if-exists! staging-ledger))
        (attempt-cleanup! #(fs/remove-tree! staging-schema))
        (reset! staging-ledger? false)
        (reset! staging-schema? false)
        {:runtime (assoc staged :schema/directory schema-directory)
         :ledger-file ledger-file
         :schema-directory schema-directory})
      (catch :default error
        (when @ledger-published?
          (attempt-cleanup! #(fs/delete-if-exists! ledger-file)))
        (when @staging-ledger?
          (attempt-cleanup! #(fs/delete-if-exists! staging-ledger)))
        (when @staging-schema?
          (attempt-cleanup! #(fs/remove-tree! staging-schema)))
        (when @schema-claimed?
          (clear-init-marker! schema-directory)
          (attempt-cleanup! #(exclusive-fs/remove-directory-if-empty! schema-directory)))
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
   still sees a fresh storage epoch. Before either mode, recover any incomplete
   initialization left by process death (resume from the durable marker, or
   roll the unadoptable epoch back)."
  [{:keys [ledger-file schema-directory initialize?]}]
  (doseq [[label value] [[:ledger-file ledger-file]
                       [:schema-directory schema-directory]]]
    (when (or (not (string? value)) (str/blank? value))
      (throw (ex-info "Explicit Clio filesystem paths required" {:path/key label}))))
  (recover-incomplete-initialization! ledger-file schema-directory)
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
