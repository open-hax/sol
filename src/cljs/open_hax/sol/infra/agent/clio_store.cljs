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
  "Create the durable init marker with a complete EDN body (exclusive create)."
  [schema-directory marker]
  (exclusive-fs/write-file-atomic!
   (init-marker-path schema-directory)
   (str (pr-str marker) "\n")))

(defn- rewrite-init-marker!
  "Update the marker through the held lock fd so the flock inode is preserved."
  [lock marker]
  (exclusive-fs/rewrite-file-in-place! lock (str (pr-str marker) "\n")))

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

(defn- ledger-owned-by-marker?
  "True when the file at ledger-file is still the inode recorded in the
   marker (the staged ledger hard-linked at publish). A replaced file at the
   same path has a different identity and must not be deleted."
  [ledger-file marker]
  (let [recorded (:ledger-identity marker)
        current (exclusive-fs/file-identity ledger-file)]
    (boolean (and recorded current (= recorded current)))))

(defn- rollback-incomplete-init!
  "Release an incomplete epoch identified by its marker while holding the
   init lock. Safe only while the schema directory holds no revision
   snapshots. The ledger is deleted only when its inode matches the marker's
   recorded identity; otherwise it is preserved."
  [{:keys [ledger-file schema-directory staging-ledger staging-schema] :as marker}]
  (when (and schema-directory (fs/exists? schema-directory)
             (empty? (schema-edn-revisions schema-directory)))
    (when (and ledger-file (fs/exists? ledger-file) (ledger-owned-by-marker? ledger-file marker))
      (attempt-cleanup! #(fs/delete-if-exists! ledger-file)))
    (when staging-ledger
      (attempt-cleanup! #(fs/delete-if-exists! staging-ledger)))
    (when staging-schema
      (attempt-cleanup! #(fs/remove-tree! staging-schema)))
    (clear-init-marker! schema-directory)
    (attempt-cleanup! #(exclusive-fs/remove-directory-if-empty! schema-directory))
    :rolled-back))

(defn- resume-incomplete-init!
  "Finish a publish interrupted after the ledger link. Requires: the init
   lock (caller), a published ledger that still matches the marker identity,
   and a staged revision. Returns :resumed when the schema commit completes."
  [{:keys [ledger-file schema-directory staging-schema staging-ledger]
    root :schema/root :as marker}]
  (let [staging-revision (when (and staging-schema root)
                           (schema-store/revision-path staging-schema root))
        target (when root (schema-store/revision-path schema-directory root))]
    (when (and ledger-file (fs/exists? ledger-file)
               (ledger-owned-by-marker? ledger-file marker)
               staging-revision target (fs/exists? staging-revision)
               (empty? (schema-edn-revisions schema-directory)))
      (try
        (exclusive-fs/link-new! staging-revision target)
        (catch :default error
          (when-not (= :already-exists (:exclusive-fs/error (ex-data error)))
            (throw error))))
      (clear-init-marker! schema-directory)
      (when staging-ledger
        (attempt-cleanup! #(fs/delete-if-exists! staging-ledger)))
      (attempt-cleanup! #(fs/remove-tree! staging-schema))
      :resumed)))

(defn- recover-markerless-empty-claim!
  "A crash between exclusive mkdir and the first marker write leaves an empty
   schema directory with no ledger. Remove it so initialize can retry."
  [ledger-file schema-directory]
  (when (and (fs/exists? schema-directory)
             (not (fs/exists? (init-marker-path schema-directory)))
             (empty? (schema-edn-revisions schema-directory))
             (not (fs/exists? ledger-file))
             (empty? (fs/list-files schema-directory)))
    (attempt-cleanup! #(exclusive-fs/remove-directory-if-empty! schema-directory))
    :rolled-back))

(defn- recover-incomplete-initialization!
  "If a prior attempt left a durable init marker, acquire its flock (so a
   live initializer is never disturbed), then resume when the ledger was
   published, or roll back when it was not / cannot be completed. Returns
   :resumed, :rolled-back, or nil."
  [ledger-file schema-directory]
  (or (recover-markerless-empty-claim! ledger-file schema-directory)
      (when (and (fs/exists? schema-directory)
                 (fs/exists? (init-marker-path schema-directory)))
        (when-let [lock (exclusive-fs/try-lock-file! (init-marker-path schema-directory))]
          (try
            (when-let [marker (read-init-marker schema-directory)]
              (cond
                (not (and (= ledger-file (:ledger-file marker))
                          (= schema-directory (:schema-directory marker))))
                nil

                (seq (schema-edn-revisions schema-directory))
                (do (clear-init-marker! schema-directory)
                    (when-let [staging-ledger (:staging-ledger marker)]
                      (attempt-cleanup! #(fs/delete-if-exists! staging-ledger)))
                    (when-let [staging-schema (:staging-schema marker)]
                      (attempt-cleanup! #(fs/remove-tree! staging-schema)))
                    :resumed)

                :else
                (or (resume-incomplete-init! marker)
                    (rollback-incomplete-init! marker))))
            (finally
              (exclusive-fs/release-lock! lock)))))))

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
   The claim is prepared privately (directory + complete marker + flock) and
   renamed onto schema-directory so no peer observes an unlocked marker.
   Marker updates go through the locked fd (same inode). Publishing is two
   exclusive hard links: ledger then schema revision. The marker records the
   ledger inode so recovery can prove ownership."
  [ledger-file schema-directory]
  (when (or (fs/exists? ledger-file) (fs/exists? schema-directory))
    (throw (ex-info "Clio initialization requires a fresh storage epoch" {})))
  (when (or (path-beneath? ledger-file schema-directory)
            (path-beneath? schema-directory ledger-file)
            (= (canonical-path ledger-file) (canonical-path schema-directory)))
    (throw (ex-info
            "Clio ledger file and schema directory must not nest"
            {:ledger-file ledger-file :schema-directory schema-directory})))
  (let [attempt (random-uuid)
        claim-directory (str schema-directory ".claim-" attempt)
        staging-ledger (str ledger-file ".init-" attempt)
        staging-schema (str schema-directory ".init-" attempt)
        staging-ledger? (atom false)
        staging-schema? (atom false)
        schema-claimed? (atom false)
        ledger-published? (atom false)
        init-lock (atom nil)]
    (try
      (fs/ensure-dir! (path/dirname schema-directory))
      (exclusive-fs/create-directory!
       claim-directory
       (fn []
         (write-init-marker! claim-directory
                             {:sol.clio/init-pending true
                              :phase :claimed
                              :ledger-file ledger-file
                              :schema-directory schema-directory})))
      (reset! init-lock (exclusive-fs/lock-file! (init-marker-path claim-directory)))
      (exclusive-fs/rename-new! claim-directory schema-directory)
      (reset! schema-claimed? true)
      (exclusive-fs/create-directory! staging-schema #(reset! staging-schema? true))
      (fs/ensure-dir! (path/dirname ledger-file))
      (exclusive-fs/create-file! staging-ledger #(reset! staging-ledger? true))
      (let [staged (runtime/open staging-schema lifecycle/catalog)
            root (:schema/root (:schema/current staged))
            _ (ledger/canonicalize-files (:schema/revisions staged) [staging-ledger])
            ledger-identity (exclusive-fs/file-identity staging-ledger)]
        (rewrite-init-marker! @init-lock
                              {:sol.clio/init-pending true
                               :phase :publishing
                               :ledger-file ledger-file
                               :schema-directory schema-directory
                               :staging-ledger staging-ledger
                               :staging-schema staging-schema
                               :schema/root root
                               :ledger-identity ledger-identity})
        (exclusive-fs/link-new! staging-ledger ledger-file)
        (reset! ledger-published? true)
        (exclusive-fs/link-new! (schema-store/revision-path staging-schema root)
                                (schema-store/revision-path schema-directory root))
        (exclusive-fs/release-lock! @init-lock)
        (reset! init-lock nil)
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
        (when @init-lock
          (exclusive-fs/release-lock! @init-lock)
          (reset! init-lock nil))
        (when @schema-claimed?
          (clear-init-marker! schema-directory)
          (attempt-cleanup! #(exclusive-fs/remove-directory-if-empty! schema-directory)))
        (when (fs/exists? claim-directory)
          (clear-init-marker! claim-directory)
          (attempt-cleanup! #(fs/remove-tree! claim-directory)))
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
   initialization left by process death. When recovery resumes a committed
   epoch, reopen it even if initialize? was requested."
  [{:keys [ledger-file schema-directory initialize?]}]
  (doseq [[label value] [[:ledger-file ledger-file]
                       [:schema-directory schema-directory]]]
    (when (or (not (string? value)) (str/blank? value))
      (throw (ex-info "Explicit Clio filesystem paths required" {:path/key label}))))
  (let [recovery (recover-incomplete-initialization! ledger-file schema-directory)]
    (cond
      (= :resumed recovery)
      (reopen! ledger-file schema-directory)

      ;; Committed epoch left after a crash past marker clear: initialize?
      ;; supervisors reopen rather than refuse non-fresh paths.
      (and initialize?
           (fs/exists? ledger-file)
           (fs/exists? schema-directory)
           (nil? (read-init-marker schema-directory))
           (seq (schema-edn-revisions schema-directory)))
      (reopen! ledger-file schema-directory)

      initialize?
      (initialize! ledger-file schema-directory)

      :else
      (reopen! ledger-file schema-directory))))

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
