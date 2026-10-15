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
   equal, so nest checks see the layout the runtime will actually use.
   Trailing separators are stripped so marker spellings compare equal."
  [p]
  (let [trimmed (str/replace (str p) #"[\\/]+$" "")
        resolved (path/resolve (if (str/blank? trimmed) (str p) trimmed))]
    (loop [cur resolved
           suffix ()]
      (if (fs/exists? cur)
        (apply path/join (exclusive-fs/realpath cur) suffix)
        (let [parent (path/dirname cur)]
          (if (= parent cur)
            resolved
            (recur parent (cons (path/basename cur) suffix))))))))

(defn- same-storage-path?
  "True when two configured path spellings resolve to the same location
   (trailing separators and relative/absolute aliases included)."
  [a b]
  (boolean
   (and (string? a) (string? b)
        (= (canonical-path a) (canonical-path b)))))

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

(defn- init-marker-tmp-path
  "replace-file! stages the next marker body here before renaming it in."
  [schema-directory]
  (str (init-marker-path schema-directory) ".tmp"))

(defn- sibling-path
  "A path beside p: suffix appended to the resolved path, so a configured
   trailing separator can never place it inside p."
  [p suffix]
  (str (path/resolve p) suffix))

(defn- init-lock-path
  "Permanent sibling lock file (never inside schema-directory, never deleted).
   Every initialization and every recovery holds it, so a claim that exists
   while it is free belongs to a dead process."
  [schema-directory]
  (sibling-path schema-directory ".sol-clio-lock"))

(defn- init-claim-path
  "Sibling ownership evidence written (under the init lock) just before the
   exclusive mkdir and removed once the in-directory marker exists. Only a
   markerless empty directory with this evidence is a crashed Sol claim."
  [schema-directory]
  (sibling-path schema-directory ".sol-clio-claim"))

(defn- write-init-claim!
  "Record the identity of the directory this attempt owns. `identity-of` is
   the path whose inode to bind (the live directory, possibly a private
   staging name that will be renamed onto schema-directory)."
  ([schema-directory] (write-init-claim! schema-directory schema-directory))
  ([schema-directory identity-of]
   (exclusive-fs/replace-file!
    (init-claim-path schema-directory)
    (str (pr-str {:sol.clio/claim (path/resolve schema-directory)
                  :identity (exclusive-fs/file-identity identity-of)})
         "\n"))))

(defn- read-claim-data
  "Parse a claim body when present and well-formed; otherwise nil."
  [claim-path]
  (when (fs/exists? claim-path)
    (try
      (let [data (reader/read-string (fs/read-text claim-path))]
        (when (map? data) data))
      (catch :default _ nil))))

(defn- claim-proves-ownership?
  "True when claim evidence (the durable file or its replace-file! `.tmp`)
   names this directory and its current filesystem identity. Accepting the
   temporary ownership record covers a crash after the body was fsynced but
   before the final rename."
  [schema-directory]
  (let [claim (init-claim-path schema-directory)
        data (or (read-claim-data claim) (read-claim-data (str claim ".tmp")))
        current (exclusive-fs/file-identity schema-directory)]
    (boolean
     (and data
          (= (path/resolve schema-directory) (:sol.clio/claim data))
          current
          (= current (:identity data))))))

(defn- clear-init-claim!
  [schema-directory]
  (attempt-cleanup! #(fs/delete-if-exists! (str (init-claim-path schema-directory) ".tmp")))
  (attempt-cleanup! #(fs/delete-if-exists! (init-claim-path schema-directory))))

(defn- with-init-lock
  "Run f holding the init lock. Initialization waits for a live peer so it can
   reopen what the peer commits; reopen-only mode never waits: when a live
   initializer holds the lock, it calls f-busy (no recovery) instead."
  ([schema-directory f]
   (let [lock (exclusive-fs/lock-file! (init-lock-path schema-directory))]
     (try
       (f)
       (finally
         (exclusive-fs/release-lock! lock)))))
  ([schema-directory f f-busy]
   (if-let [lock (exclusive-fs/try-lock-file! (init-lock-path schema-directory))]
     (try
       (f)
       (finally
         (exclusive-fs/release-lock! lock)))
     (f-busy))))

(defn- write-init-marker!
  "Crash-atomically set the durable init marker to a complete EDN body:
   process death leaves the previous complete marker or the new one, never an
   empty or partial file. Caller holds the init lock."
  [schema-directory marker]
  (exclusive-fs/replace-file!
   (init-marker-path schema-directory)
   (str (pr-str marker) "\n")))

(defn- clear-init-marker-strict!
  "Remove the marker (tmp first); failures propagate."
  [schema-directory]
  (fs/delete-if-exists! (init-marker-tmp-path schema-directory))
  (fs/delete-if-exists! (init-marker-path schema-directory)))

(defn- clear-init-marker!
  [schema-directory]
  (attempt-cleanup! #(clear-init-marker-strict! schema-directory)))

(defn- strip-trailing-seps
  [p]
  (when (string? p)
    (let [trimmed (str/replace p #"[\\/]+$" "")]
      (if (str/blank? trimmed) p trimmed))))

(defn- normalize-marker-paths
  "Strip trailing separators from path fields so file existence checks and
   ownership probes see the real ledger/schema locations."
  [marker]
  (reduce (fn [m k]
            (if (contains? m k)
              (update m k strip-trailing-seps)
              m))
          marker
          [:ledger-file :schema-directory :staging-ledger :staging-schema]))

(defn- read-init-marker
  "Return the pending-init map when present and well-formed; otherwise nil."
  [schema-directory]
  (let [marker (init-marker-path schema-directory)]
    (when (fs/exists? marker)
      (try
        (let [data (reader/read-string (fs/read-text marker))]
          (when (and (map? data) (true? (:sol.clio/init-pending data)))
            (normalize-marker-paths data)))
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

(defn- generated-staging?
  "True only for the `<configured>.init-<id>` sibling form initialize!
   generates, so a corrupted or forged marker can never aim a (recursive)
   delete at an unrelated path."
  [configured staging]
  (boolean
   (when (and (string? configured) (string? staging))
     (let [base (path/resolve configured)
           candidate (path/resolve staging)
           prefix (str (path/basename base) ".init-")
           name (path/basename candidate)]
       (and (= (path/dirname base) (path/dirname candidate))
            (str/starts-with? name prefix)
            (re-matches #"[A-Za-z0-9-]+" (subs name (count prefix))))))))

(defn- remove-staging!
  "Remove the marker's generated staging artifacts. strict? propagates
   failures (rollback must not report success over leftovers)."
  ([marker] (remove-staging! marker false))
  ([{:keys [ledger-file schema-directory staging-ledger staging-schema]} strict?]
   (let [run (if strict? (fn [f] (f)) attempt-cleanup!)]
     (when (generated-staging? ledger-file staging-ledger)
       (run #(fs/delete-if-exists! staging-ledger)))
     (when (generated-staging? schema-directory staging-schema)
       (run #(fs/remove-tree! staging-schema))))))

(defn- rollback-incomplete-init!
  "Release an incomplete epoch identified by its marker while holding the
   init lock. Safe only while the schema directory holds no revision
   snapshots. The ledger is deleted only when its inode matches the marker's
   recorded identity; otherwise it is preserved. Claim evidence is retained
   until the schema directory is actually removed, so a transient rmdir
   failure stays recoverable. A busy/kept ledger unpublication leaves the
   marker and staging in place for a later recovery."
  [{:keys [ledger-file schema-directory] :as marker}]
  (when (and schema-directory (fs/exists? schema-directory)
             (empty? (schema-edn-revisions schema-directory)))
    (let [unlink-result
          (when (and ledger-file (fs/exists? ledger-file)
                     (ledger-owned-by-marker? ledger-file marker))
            (exclusive-fs/unlink-if-empty! ledger-file))]
      (if (contains? #{:busy :kept :kept-aside} unlink-result)
        :rolled-back
        (do
          (remove-staging! marker true)
          ;; Publish claim only when absent (never rebind a mismatched claim
          ;; to a replacement directory's inode).
          (when (and (fs/exists? schema-directory)
                     (not (fs/exists? (init-claim-path schema-directory)))
                     (not (fs/exists? (str (init-claim-path schema-directory) ".tmp"))))
            (write-init-claim! schema-directory))
          (if (claim-proves-ownership? schema-directory)
            (do (clear-init-marker-strict! schema-directory)
                (let [removed (exclusive-fs/remove-directory-if-empty! schema-directory)]
                  (when (= :removed removed)
                    (clear-init-claim! schema-directory))))
            ;; Mismatched sibling claim: drop our marker, leave the directory.
            (clear-init-marker-strict! schema-directory))
          :rolled-back)))))

(defn- resume-incomplete-init!
  "Finish a publish interrupted after the ledger link. Requires: the init
   lock (caller), a published ledger that still matches the marker identity,
   and a staged revision. Returns :resumed when the schema commit completes."
  [{:keys [ledger-file schema-directory staging-schema]
    root :schema/root :as marker}]
  (let [staging-revision (when (and root (generated-staging? schema-directory staging-schema))
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
      (remove-staging! marker)
      :resumed)))

(defn- promote-claim-tmp!
  "When only the replace-file! temporary ownership record remains, promote it
   to the durable claim path so markerless recovery can prove ownership."
  [schema-directory]
  (let [claim (init-claim-path schema-directory)
        tmp (str claim ".tmp")]
    (when (and (not (fs/exists? claim)) (fs/exists? tmp) (claim-proves-ownership? schema-directory))
      (attempt-cleanup! #(exclusive-fs/rename-path! tmp claim)))))

(defn- recover-markerless-empty-claim!
  "A crash between the exclusive mkdir and the first marker rename leaves a
   schema directory holding nothing but (at most) the marker's tmp body, no
   ledger, and sibling claim evidence bound to that directory's identity.
   Under the init lock that claim is dead: remove it so initialize can
   retry. Without matching evidence an empty directory is not Sol's and is
   never removed."
  [ledger-file schema-directory]
  (promote-claim-tmp! schema-directory)
  (when (and (fs/exists? schema-directory)
             (claim-proves-ownership? schema-directory)
             (not (fs/exists? (init-marker-path schema-directory)))
             (not (fs/exists? ledger-file))
             (every? #{(path/basename (init-marker-tmp-path schema-directory))}
                     (fs/list-files schema-directory)))
    (clear-init-marker! schema-directory)
    (let [removed (exclusive-fs/remove-directory-if-empty! schema-directory)]
      (when (= :removed removed)
        (clear-init-claim! schema-directory)))
    :rolled-back))

(defn- recover-marked-initialization!
  [ledger-file schema-directory]
  (or (recover-markerless-empty-claim! ledger-file schema-directory)
      ;; An empty/partial marker (only an older truncate-and-rewrite writer
      ;; could leave one) is a dead claim too: committed history just loses
      ;; the marker; an uncommitted claim rolls back with no ledger identity,
      ;; so an unverifiable ledger is always preserved.
      (when (and (fs/exists? schema-directory)
                 (fs/exists? (init-marker-path schema-directory))
                 (nil? (read-init-marker schema-directory)))
        (if (seq (schema-edn-revisions schema-directory))
          (do (clear-init-marker! schema-directory) :resumed)
          (rollback-incomplete-init! {:ledger-file ledger-file
                                      :schema-directory schema-directory})))
      (when-let [marker (and (fs/exists? schema-directory)
                             (read-init-marker schema-directory))]
        (cond
          (not (and (same-storage-path? ledger-file (:ledger-file marker))
                    (same-storage-path? schema-directory (:schema-directory marker))))
          nil

          (seq (schema-edn-revisions schema-directory))
          (do (clear-init-marker! schema-directory)
              (remove-staging! marker)
              :resumed)

          :else
          (or (resume-incomplete-init! marker)
              (rollback-incomplete-init! marker))))))

(defn- recover-incomplete-initialization!
  "Caller holds the init lock, so no live initializer can be disturbed and a
   marker cannot vanish mid-decision. Resume when the ledger was published,
   or roll back when it was not / cannot be completed. Returns :resumed,
   :rolled-back, or nil."
  [ledger-file schema-directory]
  (let [recovery (recover-marked-initialization! ledger-file schema-directory)]
    ;; Drop claim evidence only when the schema directory is gone or the
    ;; epoch was resumed (committed history no longer needs the claim).
    (when (or (not (fs/exists? schema-directory))
              (= :resumed recovery)
              (seq (when (fs/exists? schema-directory)
                     (schema-edn-revisions schema-directory))))
      (clear-init-claim! schema-directory))
    recovery))

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

(defn- require-unnested!
  [ledger-file schema-directory]
  (when (or (path-beneath? ledger-file schema-directory)
            (path-beneath? schema-directory ledger-file)
            (= (canonical-path ledger-file) (canonical-path schema-directory)))
    (throw (ex-info
            "Clio ledger file and schema directory must not nest"
            {:ledger-file ledger-file :schema-directory schema-directory}))))

(defn- fresh-epoch-required! []
  (throw (ex-info "Clio initialization requires a fresh storage epoch" {})))

(defn- initialize!
  "Create a fresh storage epoch whose artifacts stay private until commit.
   Caller holds the sibling init lock for the whole attempt. The schema
   directory is claimed with an exclusive mkdir, so an existing path (even an
   empty directory created by another actor) fails instead of being taken
   over. Every marker phase is written crash-atomically (tmp + rename).
   Publishing is two exclusive hard links: ledger then schema revision. The
   marker records the ledger inode so recovery can prove ownership."
  [ledger-file schema-directory]
  (when (or (fs/exists? ledger-file) (fs/exists? schema-directory))
    (fresh-epoch-required!))
  (let [attempt (random-uuid)
        staging-ledger (sibling-path ledger-file (str ".init-" attempt))
        staging-schema (sibling-path schema-directory (str ".init-" attempt))
        claim {:sol.clio/init-pending true
               :ledger-file ledger-file
               :schema-directory schema-directory
               :staging-ledger staging-ledger
               :staging-schema staging-schema}
        staging-ledger? (atom false)
        staging-schema? (atom false)
        schema-claimed? (atom false)
        ledger-published? (atom false)]
    (try
      ;; Exclusive mkdir on the configured path (atomic EEXIST) then bind
      ;; claim evidence to that inode. create-directory! rolls back the mkdir
      ;; if the claim write throws; claim.tmp remains recoverable ownership.
      (try
        (exclusive-fs/create-directory! schema-directory
                                        #(do (reset! schema-claimed? true)
                                             (write-init-claim! schema-directory)))
        (catch :default error
          (if (= :already-exists (:exclusive-fs/error (ex-data error)))
            (fresh-epoch-required!)
            (throw error))))
      (write-init-marker! schema-directory (assoc claim :phase :claimed))
      (clear-init-claim! schema-directory)
      (exclusive-fs/create-directory! staging-schema #(reset! staging-schema? true))
      (fs/ensure-dir! (path/dirname ledger-file))
      (exclusive-fs/create-file! staging-ledger #(reset! staging-ledger? true))
      (let [staged (runtime/open staging-schema lifecycle/catalog)
            root (:schema/root (:schema/current staged))
            _ (ledger/canonicalize-files (:schema/revisions staged) [staging-ledger])
            ledger-identity (exclusive-fs/file-identity staging-ledger)]
        (write-init-marker! schema-directory
                            (assoc claim
                                   :phase :publishing
                                   :schema/root root
                                   :ledger-identity ledger-identity))
        (exclusive-fs/link-new! staging-ledger ledger-file)
        (reset! ledger-published? true)
        (exclusive-fs/link-new! (schema-store/revision-path staging-schema root)
                                (schema-store/revision-path schema-directory root))
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
        ;; The ledger was visible after its link: unpublish it only while
        ;; it is still our empty inode; an append by another opener keeps it.
        (let [unlink-result
              (when (and @ledger-published?
                         (exclusive-fs/same-file-identity? ledger-file staging-ledger))
                (try (exclusive-fs/unlink-if-empty! ledger-file)
                     (catch :default _ :busy)))
              ledger-retained? (contains? #{:busy :kept :kept-aside} unlink-result)]
          (if ledger-retained?
            ;; Leave marker + staging so recovery can resume or finish once
            ;; the append lock is free / the retained ledger is acknowledged.
            (when-not (read-init-marker schema-directory)
              (attempt-cleanup!
               #(write-init-marker! schema-directory
                                    (assoc claim
                                           :phase :publishing
                                           :ledger-identity
                                           (or (exclusive-fs/file-identity ledger-file)
                                               (exclusive-fs/file-identity staging-ledger))))))
            (do
              (when @staging-ledger?
                (attempt-cleanup! #(fs/delete-if-exists! staging-ledger)))
              (when @staging-schema?
                (attempt-cleanup! #(fs/remove-tree! staging-schema)))
              (when @schema-claimed?
                (when (and (fs/exists? schema-directory)
                           (empty? (schema-edn-revisions schema-directory))
                           (not (fs/exists? (init-claim-path schema-directory)))
                           (not (fs/exists? (str (init-claim-path schema-directory) ".tmp")))
                           (or (read-init-marker schema-directory)
                               (true? @schema-claimed?)))
                  ;; Claim was cleared after the marker write; republish only
                  ;; when absent so we never rebind a mismatched sibling claim.
                  (attempt-cleanup! #(write-init-claim! schema-directory)))
                (cond
                  (claim-proves-ownership? schema-directory)
                  (do (clear-init-marker! schema-directory)
                      (attempt-cleanup! #(exclusive-fs/remove-directory-if-empty! schema-directory))
                      (when-not (fs/exists? schema-directory)
                        (clear-init-claim! schema-directory)))

                  (read-init-marker schema-directory)
                  ;; Marker present but claim mismatched: drop marker only.
                  (clear-init-marker! schema-directory)

                  :else
                  (clear-init-claim! schema-directory)))
              (when-not (fs/exists? schema-directory)
                (clear-init-claim! schema-directory)))))
        (throw error)))))


(defn- reopen!
  [ledger-file schema-directory]
  ;; Upstream refuses deleted/misspelled paths instead of inventing history.
  (ledger/read-ledgers [ledger-file])
  (let [runtime (reopen-runtime schema-directory)]
    (ledger/canonicalize-files (:schema/revisions runtime) [ledger-file])
    {:runtime runtime :ledger-file ledger-file :schema-directory schema-directory}))

(defn- recovery-candidate?
  "Reopen-only mode takes the init lock just when an interrupted
   initialization may be on disk, so ordinary reopens never create the lock
   file (read-only or legacy parents keep working)."
  [schema-directory]
  (let [claim (init-claim-path schema-directory)]
    (or (fs/exists? claim)
        (fs/exists? (str claim ".tmp"))
        (and (fs/exists? schema-directory)
             (or (fs/exists? (init-marker-path schema-directory))
                 (fs/exists? (init-marker-tmp-path schema-directory)))))))

(defn- open-under-init-lock!
  [ledger-file schema-directory initialize?]
  (let [recovery (recover-incomplete-initialization! ledger-file schema-directory)]
    (cond
      (= :resumed recovery)
      (reopen! ledger-file schema-directory)

      ;; Committed epoch (a peer finished first, or a crash past marker
      ;; clear): initialize? supervisors reopen rather than refuse it.
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

(defn open!
  "Open explicitly named complete Clio history. Initialization requires new
   ledger/schema paths and is separate from ordinary append/reopen. A failed
   initialization removes only the artifacts this attempt created, so a retry
   still sees a fresh storage epoch. Initialization and recovery of an
   incomplete initialization left by process death both run under the
   sibling init lock. When recovery resumes a committed epoch, or a peer
   committed it first, reopen it even if initialize? was requested."
  [{:keys [ledger-file schema-directory initialize?]}]
  (doseq [[label value] [[:ledger-file ledger-file]
                         [:schema-directory schema-directory]]]
    (when (or (not (string? value)) (str/blank? value))
      (throw (ex-info "Explicit Clio filesystem paths required" {:path/key label}))))
  (cond
    initialize?
    (do (require-unnested! ledger-file schema-directory)
        (fs/ensure-dir! (path/dirname (path/resolve schema-directory)))
        (with-init-lock schema-directory
          #(open-under-init-lock! ledger-file schema-directory true)))

    (recovery-candidate? schema-directory)
    (with-init-lock schema-directory
      #(open-under-init-lock! ledger-file schema-directory false)
      #(reopen! ledger-file schema-directory))

    :else
    (reopen! ledger-file schema-directory)))

(defn append!
  "Delegate exact-event admission to Clio; preserve constructed data on retries.
   Holds the ledger append lock so unlink-if-empty! cannot destroy the inode
   while this writer may still append."
  [store event]
  (when-not store
    (throw (ex-info "Clio filesystem store not configured" {})))
  (let [runtime (runtime/refresh (:runtime store))
        lock (exclusive-fs/lock-file!
              (exclusive-fs/append-lock-path (:ledger-file store)))]
    (try
      (ledger/append-event! (:schema/revisions runtime) (:ledger-file store) event)
      (finally
        (exclusive-fs/release-lock! lock)))))

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
