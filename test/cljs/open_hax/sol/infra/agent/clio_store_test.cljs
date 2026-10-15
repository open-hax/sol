(ns open-hax.sol.infra.agent.clio-store-test
  (:require [cljs.reader :as reader]
            [cljs.test :refer [deftest is]]
            [clio.domain.schema :as schema]
            [clio.extern.js.crypto :as crypto]
            [clio.extern.js.fs :as fs]
            [clio.infra.event :as event]
            [clio.infra.ledger :as ledger]
            [clio.infra.runtime :as runtime]
            [clio.infra.schema-store :as schema-store]
            [clojure.string :as str]
            [open-hax.sol.clio-native-probe :as probe]
            [open-hax.sol.domain.node.path :as path]
            [open-hax.sol.extern.exclusive-fs :as exclusive-fs]
            [open-hax.sol.infra.agent.clio-store :as store]
            [open-hax.sol.infra.agent.episode-ledger :as episode]
            [open-hax.sol.infra.agent.session-store :as sessions]
            [open-hax.sol.law.lifecycle-catalog :as lifecycle]
            ["node:fs" :as node-fs]))

(defn scratch [] (path/resolve ".ημ" "sol-clio-tests" (str (random-uuid))))
(defn options [root]
  {:ledger-file (path/join root "new-epoch" "events.edn")
   :schema-directory (path/join root "new-epoch" "schemas")})
(defn candidate [canonical-store sequence causes]
  (event/make-event
   (store/current-revision canonical-store) :sol.run/started
   {:event/stream "test-episode" :event/seq sequence :event/causes causes
    :event/actor "sol.test" :event/subject "test-run"
    :event/data {:run/id "test-run" :session/id "test-session" :turn/id "test-turn"
                 :episode/id "test-episode" :event/from {:actor-id "sol.test"}
                 :delivery/mode "stream" :payload {:status "running"}}}))
(defn staging-leftovers
  "Initialization staging files (`<ledger>.init-*`) left beside the ledger."
  [ledger-file]
  (let [prefix (str (path/basename ledger-file) ".init-")]
    (filterv #(str/starts-with? % prefix) (fs/list-files (path/dirname ledger-file)))))
(defn schema-staging-leftovers
  "Initialization schema staging directories (`<schemas>.init-*`)."
  [schema-directory]
  (let [prefix (str (path/basename schema-directory) ".init-")]
    (filterv #(str/starts-with? % prefix) (fs/list-files (path/dirname schema-directory)))))
(defn visible-revisions
  "Snapshot names Clio's load-revisions would read from the directory."
  [schema-directory]
  (filterv #(str/ends-with? % ".edn") (fs/list-files schema-directory)))
(defn error-kind [f]
  (try (f) nil (catch :default error (:clio/error (ex-data error)))))

(def init-marker-name ".sol-clio-init")
(defn init-marker-path [schema-directory]
  (path/join schema-directory init-marker-name))
(defn init-lock-path
  "Sibling init lock: lives beside schema-directory, never inside it."
  [schema-directory]
  (str schema-directory ".sol-clio-lock"))
(defn ^:async promise-rejection [f]
  ;; A container preserves the returned Promise across the async try expression.
  (let [[result] (try
                   [(f)]
                   (catch :default error
                     (is false (str "Session event API threw before returning a Promise: " (.-message error)))
                     [(js/Promise.reject error)]))]
    (is (instance? js/Promise result) "Session event API returns a Promise")
    (try
      (await result)
      (is false "Expected Promise rejection")
      nil
      (catch :default error error))))

(deftest ^:async actual-lost-ack-and-reopen-test
  (let [root (scratch)]
    (try
      (let [result (await (probe/probe! root))]
        (is (= {:status :passed :events 2 :identical-retry true
                :reopen-replay true :stream-sequences [1 2]
                :session-promises true :session-errors-preserved true} result)))
      (finally (fs/remove-tree! root)))))

(deftest ^:async actual-admission-and-session-seam-test
  (let [root (scratch)]
    (try
      (let [canonical-store (store/open! (assoc (options root) :initialize? true))
            first-event (candidate canonical-store 1 [])
            session-store (sessions/create-edn-session-store
                           (path/join root "projections") canonical-store)]
        (is (= :appended (store/append! canonical-store first-event)))
        (is (= :already-present (store/append! canonical-store first-event)))
        (is (= :clio.ledger/id-collision
               (error-kind #(store/append! canonical-store
                                           (assoc-in first-event [:event/data :payload :status] "changed")))))
        (is (= :clio.ledger/concurrent-stream-write
               (error-kind #(store/append! canonical-store (candidate canonical-store 1 [])))))
        (is (= [first-event] (store/events canonical-store)))
        (let [error (await (promise-rejection
                           #(sessions/append-event! session-store "wrong-session" first-event)))]
          (is (re-find #"another Sol session" (.-message error)))
          (is (= {:session-id "wrong-session"} (ex-data error))))
        (is (= [first-event] (store/events canonical-store))))
      (finally (fs/remove-tree! root)))))

(deftest ^:async session-append-and-read-delegate-to-clio-test
  (let [root (scratch)]
    (try
      (let [canonical-store (store/open! (assoc (options root) :initialize? true))
            session-store (sessions/create-edn-session-store (path/join root "views") canonical-store)
            event (candidate canonical-store 1 [])]
        (is (true? (await (sessions/append-event! session-store "test-session" event))))
        (is (true? (await (sessions/append-event! session-store "test-session" event))))
        (is (= [event] (await (sessions/get-events session-store "test-session"))))
        (is (= [] (await (sessions/get-events session-store "other-session")))))
      (finally (fs/remove-tree! root)))))

(deftest ^:async explicit-storage-epoch-and-legacy-preservation-test
  (let [root (scratch)]
    (try
      (fs/ensure-dir! root)
      (let [old-ledger (path/join root "legacy.edn")
            old-schema (path/join root "legacy-schema.edn")
            legacy "{:envelope/version 1 :event/id \"old-arbitrary-id\"}\n"
            old-schema-bytes "{:old/schema :untouched}\n"]
        (fs/write-text! old-ledger legacy)
        (fs/write-text! old-schema old-schema-bytes)
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"fresh storage epoch"
              (store/open! {:ledger-file old-ledger
                            :schema-directory (path/join root "new-schemas") :initialize? true})))
        (let [canonical-store (store/open! (assoc (options root) :initialize? true))
              schema-dir (:schema-directory (options root))
              before (mapv #(fs/read-text (path/join schema-dir %)) (fs/list-files schema-dir))]
          (is (= [] (store/events canonical-store)))
          (store/open! (options root))
          (is (= before (mapv #(fs/read-text (path/join schema-dir %)) (fs/list-files schema-dir))))
          (is (= legacy (fs/read-text old-ledger)))
          (is (= old-schema-bytes (fs/read-text old-schema))))
        (let [error (await (promise-rejection
                           #(sessions/get-events (sessions/create-edn-session-store root) "test-session")))]
          (is (re-find #"not configured" (.-message error)))))
      (finally (fs/remove-tree! root)))))

(deftest ledger-as-ancestor-of-schema-is-rejected-before-creating-anything-test
  "When ledger-file is an ancestor of schema-directory, refuse before mkdir so
   ensure-dir! cannot leave a directory at the ledger path that blocks retries."
  (let [root (scratch)
        ledger-file (path/join root "epoch")
        schema-directory (path/join ledger-file "schemas")]
    (try
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"must not nest"
            (store/open! {:ledger-file ledger-file
                          :schema-directory schema-directory
                          :initialize? true})))
      (is (not (fs/exists? ledger-file)) "nothing created at the ledger path")
      (is (not (fs/exists? schema-directory)) "schema directory never claimed")
      (finally (fs/remove-tree! root)))))

(deftest ledger-ancestor-with-dotdot-prefixed-schema-component-is-rejected-test
  "A schema path under the ledger that uses a '..'-prefixed component
   (e.g. epoch/..d/schemas) is still nested. A string-prefix escape check
   misses it: ensure-dir! then leaves a directory at the ledger path that
   blocks every retry until manual cleanup."
  (let [root (scratch)
        ledger-file (path/join root "epoch")
        schema-directory (path/join ledger-file "..d" "schemas")]
    (try
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"must not nest"
            (store/open! {:ledger-file ledger-file
                          :schema-directory schema-directory
                          :initialize? true})))
      (is (not (fs/exists? ledger-file)) "nothing created at the ledger path")
      (is (not (fs/exists? (path/dirname schema-directory)))
          "dotdot-prefixed parent under ledger never created")
      (finally (fs/remove-tree! root)))))

(deftest failed-init-rollback-keeps-concurrent-children-under-schema-directory-test
  "Rollback deletes only this attempt's schema revision artifact. A concurrent
   nested schema directory or sibling ledger file under the claimed path is kept."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        nested (path/join schema-directory "tenant-b")
        nested-revision (path/join nested "other.edn")
        foreign-ledger (path/join schema-directory "tenant-b-ledger.edn")
        other "{:written/by \"nested initializer\"}
"
        foreign "{:written/by \"concurrent ledger\"}
"
        original-open runtime/open]
    (try
      (with-redefs [runtime/open
                    (fn [directory catalog]
                      (original-open directory catalog)
                      ;; Concurrent children appear under the claimed path.
                      (fs/ensure-dir! nested)
                      (fs/write-text! nested-revision other)
                      (fs/write-text! foreign-ledger foreign)
                      (throw (ex-info "schema materialization failed" {})))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"schema materialization failed"
              (store/open! (assoc opts :initialize? true)))))
      (is (not (fs/exists? ledger-file)) "this attempt's ledger removed")
      (is (= other (fs/read-text nested-revision))
          "nested schema history claimed under this directory is kept")
      (is (= foreign (fs/read-text foreign-ledger))
          "concurrent child ledger file is not unlinked")
      (is (fs/exists? nested) "nested directory survives scoped rollback")
      (finally (fs/remove-tree! root)))))

(deftest schema-directory-ancestor-of-ledger-is-rejected-before-creating-anything-test
  "Ledger-inside-schema is unsupported: runtime treats every EDN under the
   schema directory as a schema snapshot."
  (let [root (scratch)
        schema-directory (path/join root "epoch")
        ledger-file (path/join schema-directory "events.edn")]
    (try
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"must not nest"
            (store/open! {:ledger-file ledger-file
                          :schema-directory schema-directory
                          :initialize? true})))
      (is (not (fs/exists? schema-directory)) "nothing created")
      (is (not (fs/exists? ledger-file)))
      (finally (fs/remove-tree! root)))))

(deftest symlink-nested-ledger-under-schema-is-rejected-test
  "Lexical path-beneath misses nesting when paths meet through a symlink.
   If /alias points at /real/root, schema=/real/root/schemas and
   ledger=/alias/schemas/events.edn are physically nested; refuse before
   creating either path."
  (let [root (scratch)
        real-root (path/join root "real" "root")
        alias-root (path/join root "alias")
        schema-directory (path/join real-root "schemas")
        ledger-file (path/join alias-root "schemas" "events.edn")]
    (try
      (fs/ensure-dir! real-root)
      (.symlinkSync node-fs real-root alias-root)
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"must not nest"
            (store/open! {:ledger-file ledger-file
                          :schema-directory schema-directory
                          :initialize? true})))
      (is (not (fs/exists? schema-directory)) "schema directory never claimed")
      (is (not (fs/exists? ledger-file)) "ledger never created under the alias")
      (is (not (fs/exists? (path/join alias-root "schemas")))
          "alias schemas parent never created")
      (finally (fs/remove-tree! root)))))

(deftest realpath-host-access-belongs-in-exclusive-fs-extern-test
  "AGENTS.md: host filesystem access belongs in extern.*; domain.* stays pure.
   The nest-check realpath must not live in domain.node.fs."
  (let [domain (fs/read-text "src/cljs/open_hax/sol/domain/node/fs.cljs")
        exclusive (fs/read-text "src/cljs/open_hax/sol/extern/exclusive_fs.cljs")]
    (is (re-find #"\.realpathSync" exclusive)
        "realpathSync belongs in exclusive-fs extern")
    (is (not (re-find #"\.realpathSync" domain))
        "domain.node.fs must not call realpathSync")
    (is (re-find #"\(defn realpath" exclusive)
        "exclusive-fs exposes realpath for infra callers")))

(deftest reopen-rechecks-schema-directory-after-ledger-read-test
  "On reopen, if the schema directory exists at entry but is removed while
   read-ledgers runs, refuse missing history rather than letting runtime/open
   materialize a new schema directory."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        original-read ledger/read-ledgers]
    (try
      (store/open! (assoc opts :initialize? true))
      (with-redefs [ledger/read-ledgers
                    (fn [files]
                      (let [result (original-read files)]
                        (fs/remove-tree! schema-directory)
                        result))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"schema history"
              (store/open! opts))))
      (is (not (fs/exists? schema-directory))
          "reopen must not recreate schema history after the race")
      (is (fs/exists? ledger-file) "ledger from the original epoch remains")
      (finally (fs/remove-tree! root)))))

(deftest failed-initialization-rolls-back-only-its-own-artifacts-test
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        original-open runtime/open]
    (try
      (with-redefs [runtime/open (fn [directory _catalog]
                                   ;; Simulate a partial schema write before failure.
                                   (fs/ensure-dir! directory)
                                   (throw (ex-info "schema materialization failed" {})))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"schema materialization failed"
              (store/open! (assoc opts :initialize? true)))))
      (is (not (fs/exists? ledger-file)) "partial ledger removed")
      (is (not (fs/exists? schema-directory)) "schema directory created by the attempt removed")
      (is (identical? original-open runtime/open))
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store)) "retry still sees a fresh storage epoch"))
      (finally (fs/remove-tree! root)))))

(deftest close-failure-after-exclusive-create-rolls-back-the-ledger-test
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        original-create exclusive-fs/create-file!]
    (try
      (with-redefs [exclusive-fs/create-file!
                    (fn [p on-created]
                      ;; The exclusive open succeeded; the descriptor close fails.
                      (original-create p on-created)
                      (throw (ex-info "descriptor close failed" {})))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"descriptor close failed"
              (store/open! (assoc opts :initialize? true)))))
      (is (not (fs/exists? ledger-file)) "ledger created by this attempt removed")
      (is (= [] (staging-leftovers ledger-file)) "staging file created by this attempt removed")
      (is (not (fs/exists? schema-directory)))
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store)) "retry still sees a fresh storage epoch"))
      (finally (fs/remove-tree! root)))))

(deftest exclusive-publish-failure-never-deletes-another-attempts-ledger-test
  "Another attempt can win the ledger path between the fresh-epoch check and
   this attempt's exclusive publish. Publishing is an exclusive link, so it
   fails EEXIST and rollback removes only this attempt's private staging file
   and schema directory, never the other attempt's ledger."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        other "{:written/by \"another attempt\"}\n"
        original-link exclusive-fs/link-new!]
    (try
      (with-redefs [exclusive-fs/link-new!
                    (fn [from to]
                      (fs/write-text! to other)
                      (original-link from to))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"already exists"
              (store/open! (assoc opts :initialize? true)))))
      (is (= other (fs/read-text ledger-file)) "the other attempt's ledger is kept")
      (is (= [] (staging-leftovers ledger-file)) "this attempt's staging file removed")
      (is (not (fs/exists? schema-directory)) "this attempt's schema directory removed")
      (finally (fs/remove-tree! root)))))

(deftest initialization-keeps-the-ledger-unreachable-until-commit-test
  "Ownership must be atomic through rollback: while runtime/open and
   canonicalization run, nothing exists at the ledger path, so a concurrent
   reopen cannot adopt and append to a ledger this attempt may still unlink."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        visible (atom nil)
        reopen-error (atom nil)
        original-open runtime/open]
    (try
      (with-redefs [runtime/open
                    (fn [directory catalog]
                      (let [result (original-open directory catalog)]
                        (reset! visible (fs/exists? ledger-file))
                        (reset! reopen-error (error-kind #(ledger/read-ledgers [ledger-file])))
                        (throw (ex-info "schema materialization failed" {}))
                        result))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"schema materialization failed"
              (store/open! (assoc opts :initialize? true)))))
      (is (false? @visible) "ledger path is not published before commit")
      (is (= :clio.ledger/missing-file @reopen-error)
          "a concurrent reopen during initialization refuses instead of adopting")
      (is (not (fs/exists? ledger-file)))
      (is (= [] (staging-leftovers ledger-file)) "staging file rolled back")
      (is (not (fs/exists? schema-directory)))
      (finally (fs/remove-tree! root)))))

(deftest rollback-ownership-check-cannot-race-a-concurrent-append-test
  "An append landing between an emptiness check and the unlink must never be
   lost. Rollback may only remove artifacts no other process can reach.
   Failure is injected at the schema-revision link (after the ledger is
   published) so the unlink-if-empty! path actually runs with the append hook
   armed."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        accepted "{:accepted/by \"concurrent opener\"}\n"
        appended? (atom false)
        original-link exclusive-fs/link-new!
        original-unlink exclusive-fs/unlink-if-empty!]
    (try
      (with-redefs [exclusive-fs/link-new!
                    (fn [from to]
                      (if (= (path/resolve schema-directory) (path/dirname (path/resolve to)))
                        (throw (ex-info "schema materialization failed" {}))
                        (original-link from to)))
                    exclusive-fs/unlink-if-empty!
                    (fn [p]
                      (when (and (= p ledger-file) (fs/exists? ledger-file))
                        (fs/append-text! ledger-file accepted)
                        (reset! appended? true))
                      (original-unlink p))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"schema materialization failed"
              (store/open! (assoc opts :initialize? true)))))
      (is @appended? "append hook fired on the published-ledger unlink path")
      (is (and (fs/exists? ledger-file) (= accepted (fs/read-text ledger-file)))
          "an accepted concurrent append survives rollback")
      (finally (fs/remove-tree! root)))))

(deftest initialization-keeps-schema-history-unadoptable-until-the-ledger-commits-test
  "A process reopening a different existing ledger against the same schema
   directory must not be able to adopt this attempt's revision before commit:
   if this attempt then loses the ledger publish, rollback would strand that
   process's accepted history."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        ledger-b (path/join root "tenant-b" "events.edn")
        competing "{:written/by \"competing publisher\"}\n"
        visible-at-publish (atom nil)
        adopt-error (atom nil)
        original-link exclusive-fs/link-new!]
    (try
      (fs/ensure-dir! (path/dirname ledger-b))
      (ledger/create-ledger! ledger-b)
      (with-redefs [exclusive-fs/link-new!
                    (fn [from to]
                      (when (= to ledger-file)
                        (reset! visible-at-publish (visible-revisions schema-directory))
                        (reset! adopt-error
                                (try (store/open! {:ledger-file ledger-b :schema-directory schema-directory})
                                     nil
                                     (catch :default e (.-message e))))
                        (fs/write-text! ledger-file competing))
                      (original-link from to))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"already exists"
              (store/open! (assoc opts :initialize? true)))))
      (is (= [] @visible-at-publish) "no schema revision is visible before the ledger commits")
      (is (some->> @adopt-error (re-find #"schema history"))
          "a concurrent reopen of another ledger cannot adopt the uncommitted schema directory")
      (is (= competing (fs/read-text ledger-file)) "the competing publisher's ledger is kept")
      (is (= [] (staging-leftovers ledger-file)))
      (is (= [] (schema-staging-leftovers schema-directory)) "schema staging removed")
      (is (not (fs/exists? schema-directory)) "empty claimed schema directory released")
      (finally (fs/remove-tree! root)))))

(deftest schema-publish-failure-after-ledger-publish-rolls-back-the-epoch-test
  "The schema revision publish is the commit point. While the claimed schema
   directory holds no revision no reopen can succeed against the published
   ledger, so a failed schema publish still rolls the whole epoch back."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        original-link exclusive-fs/link-new!]
    (try
      (with-redefs [exclusive-fs/link-new!
                    (fn [from to]
                      (if (= (path/resolve schema-directory) (path/dirname (path/resolve to)))
                        (throw (ex-info "schema publish failed" {}))
                        (original-link from to)))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"schema publish failed"
              (store/open! (assoc opts :initialize? true)))))
      (is (not (fs/exists? ledger-file)) "unadoptable published ledger rolled back")
      (is (not (fs/exists? schema-directory)))
      (is (= [] (staging-leftovers ledger-file)))
      (is (= [] (schema-staging-leftovers schema-directory)))
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store)) "retry still sees a fresh storage epoch"))
      (finally (fs/remove-tree! root)))))

;; A short fs.writeSync must not leave a truncated snapshot to be published.
(deftest write-new-file-completes-short-writes-test
  (let [root (scratch)
        target (path/join root "snapshot.edn")
        text (apply str (repeat 64 "{:schema/root \"abc\"}"))
        original-write (.-writeSync node-fs)]
    (try
      (fs/ensure-dir! root)
      (set! (.-writeSync node-fs)
            (fn [fd data & args]
              ;; Accept at most 7 bytes per call, as a short write would,
              ;; whether called with a string or a buffer slice.
              (let [bytes (if (string? data)
                            (.from js/Buffer data "utf8")
                            (let [[offset length] args]
                              (.subarray data (or offset 0) (+ (or offset 0) (or length (.-length data))))))]
                (.call original-write node-fs fd (.subarray bytes 0 (min 7 (.-length bytes)))))))
      (exclusive-fs/write-new-file! target text (fn [] nil))
      (set! (.-writeSync node-fs) original-write)
      (is (= text (fs/read-text target)) "every byte written before close")
      (finally
        (set! (.-writeSync node-fs) original-write)
        (fs/remove-tree! root)))))

(deftest successful-initialization-publishes-the-ledger-and-removes-staging-test
  (let [root (scratch)
        {:keys [ledger-file] :as opts} (options root)]
    (try
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= "" (fs/read-text ledger-file)) "published ledger is the empty new epoch")
        (is (= [] (staging-leftovers ledger-file)) "staging name unlinked after publish")
        (is (= :appended (store/append! canonical-store (candidate canonical-store 1 [])))))
      (finally (fs/remove-tree! root)))))

(deftest reopen-never-calls-a-schema-creating-operation-test
  "Reopen must load schema history through non-creating operations only:
   runtime/open and ensure-revision! recreate a directory that vanished after
   validation, silently inventing missing history."
  (let [root (scratch)
        opts (options root)
        creating-calls (atom [])]
    (try
      (store/open! (assoc opts :initialize? true))
      (with-redefs [runtime/open (fn [& args] (swap! creating-calls conj [:runtime/open args])
                                   (throw (ex-info "creating open" {})))
                    schema-store/ensure-revision! (fn [& args] (swap! creating-calls conj [:ensure-revision! args])
                                                    (throw (ex-info "creating ensure" {})))
                    fs/ensure-dir! (fn [& args] (swap! creating-calls conj [:ensure-dir! args])
                                     (throw (ex-info "creating mkdir" {})))]
        (let [reopened (store/open! opts)]
          (is (= [] (store/events reopened)))))
      (is (= [] @creating-calls) "reopen performed no creating schema operation")
      (finally (fs/remove-tree! root)))))

(deftest reopen-does-not-recreate-a-schema-directory-removed-after-load-test
  "If the schema directory vanishes after its history was loaded, nothing
   recreates it: reopen invents no history."
  (let [root (scratch)
        {:keys [schema-directory] :as opts} (options root)
        original-load schema-store/load-revisions]
    (try
      (store/open! (assoc opts :initialize? true))
      (with-redefs [schema-store/load-revisions
                    (fn [directory]
                      (let [result (original-load directory)]
                        (fs/remove-tree! schema-directory)
                        result))]
        (try (store/open! opts) (catch :default _ nil)))
      (is (not (fs/exists? schema-directory)) "reopen never recreated the schema directory")
      (finally (fs/remove-tree! root)))))

(defn- evolved-revision []
  (schema/materialize crypto/sha256 (dissoc lifecycle/catalog :sol.turn/completed)))


(deftest eexist-host-code-belongs-in-exclusive-fs-extern-test
  "AGENTS.md: nothing above extern handles a raw host object. Infra must not
   decode Node's `.code`; exclusive-fs classifies EEXIST as shaped data."
  (let [infra (fs/read-text "src/cljs/open_hax/sol/infra/agent/clio_store.cljs")
        exclusive (fs/read-text "src/cljs/open_hax/sol/extern/exclusive_fs.cljs")]
    (is (not (re-find #"\(\.-code " infra))
        "infra must not read Node error .code")
    (is (re-find #":already-exists" exclusive)
        "exclusive-fs classifies EEXIST as :already-exists")))

(deftest link-new-classifies-eexist-as-shaped-data-test
  (let [root (scratch)
        from (path/join root "from")
        to (path/join root "to")]
    (try
      (fs/ensure-dir! root)
      (fs/write-text! from "a\n")
      (fs/write-text! to "b\n")
      (let [error (try (exclusive-fs/link-new! from to) nil (catch :default e e))]
        (is (= :already-exists (:exclusive-fs/error (ex-data error)))
            "EEXIST becomes shaped :already-exists")
        (is (= (str to) (:path (ex-data error)))))
      (finally (fs/remove-tree! root)))))

(deftest concurrent-evolution-accepts-peer-publish-without-host-code-test
  "When another opener already published the same evolved root, link-new!
   reports :already-exists and reopen accepts the peer's snapshot."
  (let [root (scratch)
        {:keys [schema-directory] :as opts} (options root)
        evolved (evolved-revision)
        original-link exclusive-fs/link-new!]
    (try
      (store/open! (assoc opts :initialize? true))
      (with-redefs [store/local-revision (delay evolved)
                    exclusive-fs/link-new!
                    (fn [from to]
                      ;; Peer published the same root first.
                      (original-link from to)
                      (original-link from to))]
        (let [reopened (store/open! opts)]
          (is (= (:schema/root evolved) (:schema/root (store/current-revision reopened))))
          (is (contains? (set (fs/list-files schema-directory))
                         (str (:schema/root evolved) ".edn")))))
      (finally (fs/remove-tree! root)))))

(deftest process-death-after-ledger-publish-resumes-from-marker-test
  "Process death after the ledger hard-link and before the schema revision
   link leaves a published ledger, an empty claimed schema directory, and
   private staging. A durable init marker lets the next open resume."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        original-link exclusive-fs/link-new!
        mid (atom nil)]
    (try
      (with-redefs [exclusive-fs/link-new!
                    (fn [from to]
                      (if (= to ledger-file)
                        (do (original-link from to)
                            (reset! mid
                                    {:ledger (fs/read-text ledger-file)
                                     :marker (when (fs/exists? (init-marker-path schema-directory))
                                               (fs/read-text (init-marker-path schema-directory)))
                                     :staging-schema
                                     (first (schema-staging-leftovers schema-directory))
                                     :staging-schema-files
                                     (let [d (first (schema-staging-leftovers schema-directory))]
                                       (when d
                                         (let [dir (path/join (path/dirname schema-directory) d)]
                                           (into {} (for [f (fs/list-files dir)]
                                                      [f (fs/read-text (path/join dir f))])))))
                                     :staging-ledger (first (staging-leftovers ledger-file))
                                     :staging-ledger-bytes
                                     (let [f (first (staging-leftovers ledger-file))]
                                       (when f
                                         (fs/read-text (path/join (path/dirname ledger-file) f))))})
                            (throw (ex-info "process killed" {})))
                        (original-link from to)))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"process killed"
              (store/open! (assoc opts :initialize? true)))))
      ;; Catch rolled the attempt back; restore the mid-publish death state.
      (let [{:keys [marker staging-schema staging-schema-files
                    staging-ledger staging-ledger-bytes]} @mid]
        (is (some? marker) "initialization writes a durable marker before publish")
        (when marker
          (fs/ensure-dir! (path/dirname ledger-file))
          (fs/ensure-dir! schema-directory)
          (when staging-schema
            (let [dir (path/join (path/dirname schema-directory) staging-schema)]
              (fs/ensure-dir! dir)
              (doseq [[f bytes] staging-schema-files]
                (fs/write-text! (path/join dir f) bytes))))
          ;; Restore ledger as a hard link of the restored staging file and
          ;; rewrite the marker with that inode so resume does not depend on
          ;; the filesystem reusing the freed identity after rollback.
          (let [staged (path/join (path/dirname ledger-file) staging-ledger)]
            ;; Throw inside link-new! can leave ledger-file behind because
            ;; ledger-published? is set only after link-new! returns; clear it
            ;; so the restored hard link is the sole name we control.
            (fs/delete-if-exists! ledger-file)
            (fs/write-text! staged staging-ledger-bytes)
            (.linkSync node-fs staged ledger-file)
            (fs/write-text! (init-marker-path schema-directory)
                            (str (pr-str (assoc (reader/read-string marker)
                                               :ledger-identity
                                               (exclusive-fs/file-identity ledger-file)))
                                 "\n")))
          (let [canonical-store (store/open! (assoc opts :initialize? true))]
            (is (= [] (store/events canonical-store))
                "initialize? after resume reopens the committed epoch")
            (is (not (fs/exists? (init-marker-path schema-directory))) "marker cleared after resume")
            (is (seq (visible-revisions schema-directory)) "schema revision published")
            (is (= [] (staging-leftovers ledger-file)))
            (is (= [] (schema-staging-leftovers schema-directory))))))
      (finally (fs/remove-tree! root)))))

(deftest process-death-without-staging-rolls-back-via-marker-test
  "If staging is gone after process death, the marker still identifies an
   incomplete epoch so startup can roll it back and initialize fresh. The
   ledger is deleted only when its inode matches the marker identity."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)]
    (try
      (fs/ensure-dir! (path/dirname ledger-file))
      (fs/write-text! ledger-file "")
      (fs/ensure-dir! schema-directory)
      (fs/write-text! (init-marker-path schema-directory)
                      (str (pr-str {:sol.clio/init-pending true
                                    :phase :publishing
                                    :ledger-file ledger-file
                                    :schema-directory schema-directory
                                    :staging-ledger (str ledger-file ".init-dead")
                                    :staging-schema (str schema-directory ".init-dead")
                                    :schema/root "deadroot"
                                    :ledger-identity (exclusive-fs/file-identity ledger-file)})
                           "
"))
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store))
            "initialize rolls back the incomplete epoch then creates a fresh one")
        (is (not (fs/exists? (init-marker-path schema-directory))))
        (is (seq (visible-revisions schema-directory))))
      (finally (fs/remove-tree! root)))))

(deftest reopen-persists-an-evolved-revision-into-existing-history-test
  "A source catalog edit still adds its revision on reopen, written only into
   the existing schema directory."
  (let [root (scratch)
        {:keys [schema-directory] :as opts} (options root)
        evolved (evolved-revision)]
    (try
      (store/open! (assoc opts :initialize? true))
      (let [before (set (fs/list-files schema-directory))]
        (with-redefs [store/local-revision (delay evolved)]
          (let [reopened (store/open! opts)]
            (is (= (:schema/root evolved) (:schema/root (store/current-revision reopened))))
            (is (= (conj before (str (:schema/root evolved) ".edn"))
                   (set (fs/list-files schema-directory)))
                "evolved revision added beside the original history")
            (is (= #{(:schema/root @(delay (schema/materialize crypto/sha256 lifecycle/catalog)))
                     (:schema/root evolved)}
                   (set (map :schema/root (:schema/revisions (:runtime reopened)))))))))
      (finally (fs/remove-tree! root)))))

(deftest reopen-evolution-refuses-when-schema-directory-vanishes-before-write-test
  (let [root (scratch)
        {:keys [schema-directory] :as opts} (options root)
        original-load schema-store/load-revisions
        removed? (atom false)]
    (try
      (store/open! (assoc opts :initialize? true))
      (with-redefs [store/local-revision (delay (evolved-revision))
                    schema-store/load-revisions
                    (fn [directory]
                      (let [result (original-load directory)]
                        (when-not @removed?
                          (reset! removed? true)
                          (fs/remove-tree! schema-directory))
                        result))]
        (is (thrown? js/Error (store/open! opts))))
      (is (not (fs/exists? schema-directory))
          "evolution write never recreates a vanished schema directory")
      (finally (fs/remove-tree! root)))))

(deftest failed-initialization-keeps-a-schema-directory-another-attempt-claimed-test
  ;; Another initializer already owns schema-directory when this attempt tries
  ;; to expose its private claim via rename; their directory must be kept.
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        other-revision (path/join schema-directory "other.edn")
        other "{:written/by \"another initializer\"}\n"]
    (try
      (fs/ensure-dir! schema-directory)
      (fs/write-text! other-revision other)
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"fresh storage epoch"
            (store/open! (assoc opts :initialize? true)))
          "existing peer-owned schema directory fails the fresh-epoch check")
      (is (not (fs/exists? ledger-file)) "this attempt never created a ledger")
      (is (= other (fs/read-text other-revision))
          "the schema directory another initializer claimed is kept")
      (finally (fs/remove-tree! root)))))

(deftest ensure-configured-refuses-paths-that-differ-from-the-open-store-test
  (let [root (scratch)]
    (try
      (let [config (store/configure!
                    {:clio/ledger-file (:ledger-file (options root))
                     :clio/schema-directory (:schema-directory (options root))
                     :clio/initialize? true})]
        (is (identical? (:clio/store config) (:clio/store (store/ensure-configured! config)))
            "same paths keep the open store")
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"full restart"
              (store/ensure-configured!
               (assoc config :clio/ledger-file (path/join root "other.edn")))))
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"full restart"
              (store/ensure-configured!
               (assoc config :clio/schema-directory (path/join root "other-schemas")))))
        (is (not (fs/exists? (path/join root "other.edn")))))
      (finally (fs/remove-tree! root)))))

(deftest ensure-configured-tolerates-legacy-store-without-schema-directory-test
  "Hot reload can carry a pre-PR store map that only recorded :ledger-file.
   Recover the schema directory from the runtime when the top-level key is absent."
  (let [root (scratch)
        ledger (:ledger-file (options root))
        schema (:schema-directory (options root))
        legacy-store {:runtime {:schema/directory schema :schema/current :legacy}
                      :ledger-file ledger}
        config {:clio/ledger-file ledger
                :clio/schema-directory schema
                :clio/store legacy-store}]
    (try
      (is (identical? legacy-store (:clio/store (store/ensure-configured! config)))
          "legacy store is kept when ledger and recovered schema match")
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"full restart"
            (store/ensure-configured!
             (assoc config :clio/ledger-file (path/join root "other.edn"))))
          "a real ledger change still requires a full restart")
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"full restart"
            (store/ensure-configured!
             (assoc config :clio/schema-directory (path/join root "other-schemas"))))
          "a schema path change recovered from runtime still requires a full restart")
      (finally (fs/remove-tree! root)))))

(deftest ensure-configured-rejects-unverifiable-legacy-schema-path-test
  "When the open store cannot name its schema directory, a requested schema
   path is refused rather than treated as unchanged."
  (let [root (scratch)
        ledger (:ledger-file (options root))
        schema (:schema-directory (options root))
        legacy-store {:runtime :opaque :ledger-file ledger}
        config {:clio/ledger-file ledger
                :clio/schema-directory schema
                :clio/store legacy-store}]
    (try
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"full restart"
            (store/ensure-configured! config)))
      (finally (fs/remove-tree! root)))))

(deftest path-beneath-ignores-absolute-cross-drive-relatives-test
  (let [root (scratch)
        ledger (path/join root "events.edn")
        schema (path/join root "schemas")]
    (try
      (with-redefs [path/relative (fn [_ _] "D:\\schemas")
                    path/absolute? (fn [p] (boolean (or (re-find #"^/" (str p))
                                                        (re-find #"^[A-Za-z]:" (str p)))))]
        ;; Sibling layout would nest-check both directions; absolute relatives
        ;; must not spuriously trip the guard before exclusive creates run.
        (let [created (atom false)]
          (with-redefs [exclusive-fs/create-directory!
                        (fn [p on-created]
                          ;; Real create-directory! mkdirs; keep that so the
                          ;; immediate init marker write has a home.
                          (.mkdirSync node-fs (str p))
                          (reset! created true)
                          (on-created)
                          p)
                        exclusive-fs/create-file!
                        (fn [p on-created]
                          (on-created)
                          p)
                        runtime/open
                        (fn [_ _] (throw (ex-info "stop after nest checks" {})))]
            (is (thrown-with-msg? cljs.core/ExceptionInfo #"stop after nest checks"
                  (store/open! {:ledger-file ledger
                                :schema-directory schema
                                :initialize? true})))
            (is @created "nest guard allowed sibling paths when relative was absolute"))))
      (finally (fs/remove-tree! root)))))

(deftest failed-reopen-leaves-existing-history-test
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)]
    (try
      (store/open! (assoc opts :initialize? true))
      (let [ledger-bytes (fs/read-text ledger-file)]
        (with-redefs [schema-store/load-revisions (fn [_] (throw (ex-info "reopen failed" {})))]
          (is (thrown-with-msg? cljs.core/ExceptionInfo #"reopen failed" (store/open! opts))))
        (is (= ledger-bytes (fs/read-text ledger-file)))
        (is (fs/exists? schema-directory)))
      (finally (fs/remove-tree! root)))))

(deftest ^:async unconfigured-session-append-rejects-test
  (let [root (scratch)
        session-store (sessions/create-edn-session-store root)
        event (candidate nil 1 [])
        error (await (promise-rejection
                       #(sessions/append-event! session-store "test-session" event)))]
    (is (re-find #"not configured" (.-message error)))
    (is (= {} (ex-data error)))
    (is (not (fs/exists? root)))))

(deftest ^:async actual-clio-session-errors-reject-and-preserve-history-test
  (let [root (scratch)]
    (try
      (let [canonical-store (store/open! (assoc (options root) :initialize? true))
            session-store (sessions/create-edn-session-store (path/join root "views") canonical-store)
            event (candidate canonical-store 1 [])
            ledger-file (:ledger-file canonical-store)]
        (is (true? (await (sessions/append-event! session-store "test-session" event))))
        (let [before (fs/read-text ledger-file)
              collision (assoc-in event [:event/data :payload :status] "changed")
              error (await (promise-rejection
                             #(sessions/append-event! session-store "test-session" collision)))]
          (is (= :clio.ledger/id-collision (:clio/error (ex-data error))))
          (is (= before (fs/read-text ledger-file)))
          (is (= [event] (await (sessions/get-events session-store "test-session")))))
        (fs/append-text! ledger-file "{:broken\n")
        (let [before (fs/read-text ledger-file)
              append-error (await (promise-rejection
                                    #(sessions/append-event! session-store "test-session" event)))
              read-error (await (promise-rejection
                                  #(sessions/get-events session-store "test-session")))]
          (is (= :clio.ledger/invalid-edn (:clio/error (ex-data append-error))))
          (is (= :clio.ledger/invalid-edn (:clio/error (ex-data read-error))))
          (is (= before (fs/read-text ledger-file)))))
      (finally (fs/remove-tree! root)))))

(deftest missing-path-and-malformed-records-are-not-empty-test
  (let [root (scratch)]
    (try
      (is (= :clio.ledger/missing-file (error-kind #(store/open! (options root)))))
      (let [canonical-store (store/open! (assoc (options root) :initialize? true))]
        (fs/append-text! (:ledger-file canonical-store) "{:broken\n")
        (is (= :clio.ledger/invalid-edn (error-kind #(store/events canonical-store))))
        (is (= :clio.ledger/invalid-edn
               (error-kind #(store/append! canonical-store (candidate canonical-store 1 [])))))
        (fs/delete-if-exists! (:ledger-file canonical-store))
        (is (= :clio.ledger/missing-file (error-kind #(store/events canonical-store)))))
      (finally (fs/remove-tree! root)))))

(deftest missing-schema-directory-is-not-created-on-reopen-test
  (let [root (scratch)]
    (try
      (fs/ensure-dir! (path/dirname (:ledger-file (options root))))
      (ledger/create-ledger! (:ledger-file (options root)))
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"schema history"
            (store/open! (options root))))
      (is (not (fs/exists? (:schema-directory (options root)))))
      (finally (fs/remove-tree! root)))))

(deftest upstream-schema-and-complete-history-refusals-test
  (let [root (scratch)]
    (try
      (let [canonical-store (store/open! (assoc (options root) :initialize? true))
            first-event (candidate canonical-store 1 [])]
        (is (some? (error-kind #(store/append! canonical-store
                                  (assoc-in first-event [:event/schema :schema/root] (apply str (repeat 64 "0")))))))
        ;; Clio append supports partition-local admission, not full causal validation.
        (store/append! canonical-store (candidate canonical-store 2 []))
        (is (some? (error-kind #(store/events canonical-store)))))
      (finally (fs/remove-tree! root)))))

(deftest ^:async pending-write-is-resolved-before-compensation-test
  (let [root (scratch)]
    (try
      (let [canonical-store (store/open! (assoc (options root) :initialize? true))
            attempts* (atom 0)
            e (episode/create-episode
               {:clio/store canonical-store
                :clio/append! (fn [event]
                                (let [result (store/append! canonical-store event)]
                                  (if (= 1 (swap! attempts* inc))
                                    (throw (ex-info "lost ack" {})) result)))}
               {:run-id "test-run" :session-id "test-session"})]
        (try (await (episode/emit! e :sol.run/started {})) (catch :default _ nil))
        (await (episode/emit! e :sol.run/failed {:error "start acknowledgement lost"}))
        (is (= [:sol.run/started :sol.run/failed] (mapv :event/type (store/events canonical-store))))
        (is (= [1 2] (mapv :event/seq (store/events canonical-store)))))
      (finally (fs/remove-tree! root)))))


(deftest resume-requires-published-ledger-owned-by-marker-test
  "Recovery must not publish the staged schema while the live initializer has
   not yet linked the ledger: that races the owner and can strand a schema
   revision with no usable ledger."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        staging-schema (str schema-directory ".init-peer")
        root-id "abc"
        revision (path/join staging-schema (str root-id ".edn"))]
    (try
      (fs/ensure-dir! schema-directory)
      (fs/ensure-dir! staging-schema)
      (fs/write-text! revision "{:schema/root \"abc\" :schema/catalog {}}\n")
      (fs/write-text! (init-marker-path schema-directory)
                      (str (pr-str {:sol.clio/init-pending true
                                    :phase :publishing
                                    :ledger-file ledger-file
                                    :schema-directory schema-directory
                                    :staging-ledger (str ledger-file ".init-peer")
                                    :staging-schema staging-schema
                                    :schema/root root-id
                                    ;; No ledger-identity and no ledger file:
                                    ;; resume must refuse.
                                    })
                           "\n"))
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store))
            "without a published ledger, recovery rolls back then initializes fresh")
        (is (not (fs/exists? staging-schema))
            "peer staging removed rather than resumed into the claim")
        (is (not (fs/exists? (init-marker-path schema-directory)))))
      (finally (fs/remove-tree! root)))))

(deftest rollback-preserves-ledger-when-inode-does-not-match-marker-test
  ;; A stale marker must not delete a newer initializer's ledger at the same path.
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        foreign "foreign-epoch\n"]
    (try
      (fs/ensure-dir! (path/dirname ledger-file))
      (fs/write-text! ledger-file foreign)
      (fs/ensure-dir! schema-directory)
      (fs/write-text! (init-marker-path schema-directory)
                      (str (pr-str {:sol.clio/init-pending true
                                    :phase :publishing
                                    :ledger-file ledger-file
                                    :schema-directory schema-directory
                                    :staging-ledger (str ledger-file ".init-stale")
                                    :staging-schema (str schema-directory ".init-stale")
                                    :schema/root "stale"
                                    ;; Deliberately wrong identity.
                                    :ledger-identity {:dev -1 :ino -1}})
                           "\n"))
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"fresh storage epoch"
            (store/open! (assoc opts :initialize? true))))
      (is (= foreign (fs/read-text ledger-file))
          "foreign ledger at the marker path is preserved")
      (is (not (fs/exists? schema-directory))
          "empty incomplete schema claim still released")
      (finally (fs/remove-tree! root)))))

(deftest schema-directory-is-claimed-only-under-the-init-lock-test
  "The schema directory is created (exclusive mkdir) and marked only while the
   sibling init lock is held, so recovery can never mistake a live claim for
   a crashed one. A crash at that point leaves nothing a retry trips over."
  (let [root (scratch)
        {:keys [schema-directory] :as opts} (options root)
        original-create exclusive-fs/create-directory!
        saw (atom nil)]
    (try
      (with-redefs [exclusive-fs/create-directory!
                    (fn [p on-created]
                      (if (= p schema-directory)
                        (do (reset! saw {:peer-lock (exclusive-fs/try-lock-file!
                                                     (init-lock-path schema-directory))})
                            (exclusive-fs/release-lock! (:peer-lock @saw))
                            (throw (ex-info "crash at claim" {})))
                        (original-create p on-created)))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"crash at claim"
              (store/open! (assoc opts :initialize? true)))))
      (is (some? @saw) "schema directory claimed through exclusive create")
      (is (nil? (:peer-lock @saw)) "a peer cannot take the init lock during the claim")
      (is (not (fs/exists? schema-directory)) "claim never exposed")
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store))))
      (finally (fs/remove-tree! root)))))

(deftest initialize-reopens-committed-epoch-without-marker-test
  "If initialize committed then died after clearing the marker, a restart
   with initialize? reopens the epoch instead of refusing non-fresh paths."
  (let [root (scratch)
        {:keys [schema-directory] :as opts} (options root)]
    (try
      (store/open! (assoc opts :initialize? true))
      (is (nil? (when (fs/exists? (init-marker-path schema-directory))
                  (fs/read-text (init-marker-path schema-directory)))))
      (let [again (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events again))
            "initialize? reopens the already-committed epoch"))
      (finally (fs/remove-tree! root)))))

(deftest hot-reload-reuses-transport-and-refuses-path-change-test
  (let [root (scratch)]
    (try
      (let [config (store/configure!
                    {:clio/ledger-file (:ledger-file (options root))
                     :clio/schema-directory (:schema-directory (options root))
                     :clio/initialize? true})
            reload-config (store/reconfigure! config (dissoc config :clio/store))]
        (is (identical? (:clio/store config) (:clio/store reload-config)))
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"full restart"
              (store/reconfigure! config
                                  (assoc config :clio/ledger-file (path/join root "other.edn")))))
        (is (not (fs/exists? (path/join root "other.edn")))))
      (finally (fs/remove-tree! root)))))

(defn- snapshot-tree
  "Byte-exact snapshot of every directory and file under root (process-death
   emulation: what a kill -9 at this instant would leave on disk)."
  [root]
  (letfn [(walk [dir]
            (into [[:dir dir]]
                  (mapcat (fn [entry]
                            (let [p (path/join dir (.-name entry))]
                              (if (.isDirectory entry)
                                (walk p)
                                [[:file p (.readFileSync node-fs p)]])))
                          (.readdirSync node-fs dir #js {:withFileTypes true}))))]
    (walk root)))

(defn- restore-tree!
  [root snapshot]
  (fs/remove-tree! root)
  (doseq [[kind p bytes] snapshot]
    (case kind
      :dir (.mkdirSync node-fs p #js {:recursive true})
      :file (.writeFileSync node-fs p bytes))))

(deftest process-death-during-marker-phase-update-stays-recoverable-test
  "Process death while the marker moves to its publishing phase must never
   leave an empty or partial marker: the next initialize? recovers the claim
   instead of wedging on a non-fresh schema directory."
  (let [root (scratch)
        {:keys [schema-directory] :as opts} (options root)
        original-open runtime/open
        original-write (.-writeSync node-fs)
        opened? (atom false)
        death (atom nil)]
    (try
      (with-redefs [runtime/open (fn [directory catalog]
                                   (let [result (original-open directory catalog)]
                                     (reset! opened? true)
                                     result))]
        (set! (.-writeSync node-fs)
              (fn [& args]
                (if (and @opened? (nil? @death))
                  (do (reset! death (snapshot-tree root))
                      (throw (ex-info "process killed during marker update" {})))
                  (.apply original-write node-fs (into-array args)))))
        (try
          (is (thrown-with-msg? cljs.core/ExceptionInfo #"process killed during marker update"
                (store/open! (assoc opts :initialize? true))))
          (finally (set! (.-writeSync node-fs) original-write))))
      (is (some? @death) "death injected during the phase update")
      (restore-tree! root @death)
      (is (fs/exists? schema-directory) "claimed schema directory survives the kill")
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store))
            "restart recovers the interrupted claim and initializes a fresh epoch")
        (is (not (fs/exists? (init-marker-path schema-directory)))))
      (finally
        (set! (.-writeSync node-fs) original-write)
        (fs/remove-tree! root)))))

(deftest initialization-never-takes-over-a-concurrently-created-schema-directory-test
  "Another actor creating an empty schema-directory after the freshness check
   must make this attempt fail; publication never replaces that directory."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        original-create exclusive-fs/create-directory!
        foreign (atom nil)]
    (try
      (with-redefs [exclusive-fs/create-directory!
                    (fn [p on-created]
                      (when (nil? @foreign)
                        (fs/ensure-dir! schema-directory)
                        (reset! foreign (exclusive-fs/file-identity schema-directory)))
                      (original-create p on-created))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"fresh storage epoch"
              (store/open! (assoc opts :initialize? true)))))
      (is (= @foreign (exclusive-fs/file-identity schema-directory))
          "the concurrently created directory is kept, not replaced")
      (is (not (fs/exists? ledger-file)) "this attempt never published a ledger")
      (finally (fs/remove-tree! root)))))

(deftest marker-vanishing-before-lock-acquisition-reopens-the-epoch-test
  "A concurrent initializer that finishes (and clears its marker) just before
   recovery takes its lock is a completed race, not a startup failure."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        original-lock exclusive-fs/lock-file!
        original-try-lock exclusive-fs/try-lock-file!
        peer-finishes! #(fs/delete-if-exists! (init-marker-path schema-directory))]
    (try
      (store/open! (assoc opts :initialize? true))
      ;; The peer is about to clear its marker on a committed epoch.
      (fs/write-text! (init-marker-path schema-directory)
                      (str (pr-str {:sol.clio/init-pending true
                                    :phase :publishing
                                    :ledger-file ledger-file
                                    :schema-directory schema-directory})
                           "\n"))
      (with-redefs [exclusive-fs/lock-file! (fn [p] (peer-finishes!) (original-lock p))
                    exclusive-fs/try-lock-file! (fn [p] (peer-finishes!) (original-try-lock p))]
        (let [canonical-store (store/open! (assoc opts :initialize? true))]
          (is (= [] (store/events canonical-store))
              "initialize? reopens the epoch the peer just committed")))
      (finally (fs/remove-tree! root)))))

(deftest unreadable-marker-on-an-uncommitted-claim-is-rolled-back-test
  "An empty or partial marker (e.g. left by an older truncate-and-rewrite
   writer) on a claim with no revision snapshots is rolled back under the init
   lock instead of wedging startup; an unverifiable ledger is preserved."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)]
    (try
      (fs/ensure-dir! schema-directory)
      (fs/write-text! (init-marker-path schema-directory) "{:sol.clio/init-pending tr")
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store))
            "unreadable marker on an empty claim: roll back, then initialize fresh")
        (is (not (fs/exists? (init-marker-path schema-directory)))))
      (fs/remove-tree! root)
      (fs/ensure-dir! (path/dirname ledger-file))
      (fs/write-text! ledger-file "foreign\n")
      (fs/ensure-dir! schema-directory)
      (fs/write-text! (init-marker-path schema-directory) "")
      (is (thrown-with-msg? cljs.core/ExceptionInfo #"fresh storage epoch"
            (store/open! (assoc opts :initialize? true))))
      (is (= "foreign\n" (fs/read-text ledger-file))
          "a ledger whose identity cannot be verified is never deleted")
      (finally (fs/remove-tree! root)))))

(deftest recovery-never-deletes-staging-paths-a-marker-did-not-generate-test
  "A forged or corrupted marker naming an unrelated staging path must not make
   recovery recursively delete it."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        victim (path/join root "unrelated")
        victim-file (path/join victim "keep.txt")]
    (try
      (fs/ensure-dir! victim)
      (fs/write-text! victim-file "keep\n")
      (fs/ensure-dir! schema-directory)
      (fs/write-text! (init-marker-path schema-directory)
                      (str (pr-str {:sol.clio/init-pending true
                                    :phase :publishing
                                    :ledger-file ledger-file
                                    :schema-directory schema-directory
                                    :staging-ledger victim-file
                                    :staging-schema victim
                                    :schema/root "forged"})
                           "\n"))
      (store/open! (assoc opts :initialize? true))
      (is (= "keep\n" (fs/read-text victim-file)) "unrelated directory is never removed")
      (finally (fs/remove-tree! root)))))

(deftest file-identity-is-lossless-test
  ;; Device/inode identities keep full 64-bit precision (bigint stats).
  (let [root (scratch)
        p (path/join root "f")]
    (try
      (fs/ensure-dir! root)
      (fs/write-text! p "x")
      (let [st (.statSync node-fs p #js {:bigint true})]
        (is (= {:dev (str (.-dev st)) :ino (str (.-ino st))}
               (exclusive-fs/file-identity p))))
      (finally (fs/remove-tree! root)))))

(deftest trailing-separator-schema-path-keeps-lock-and-staging-beside-it-test
  (let [root (scratch)
        {:keys [ledger-file schema-directory]} (options root)
        opts {:ledger-file ledger-file :schema-directory (str schema-directory "/")}]
    (try
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store)) "initialization succeeds")
        (is (fs/exists? (init-lock-path schema-directory)) "lock is a sibling")
        (is (= [] (filterv #(str/starts-with? % ".") (fs/list-files schema-directory)))
            "nothing Sol-private is left inside the schema directory"))
      (finally (fs/remove-tree! root)))))

(deftest pre-existing-empty-schema-directory-is-never-adopted-test
  ;; An empty schema directory Sol did not claim is refused, not deleted.
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)]
    (try
      (fs/ensure-dir! schema-directory)
      (let [identity (exclusive-fs/file-identity schema-directory)]
        (is (thrown? js/Error (store/open! opts)) "reopen refuses empty history")
        (is (= identity (exclusive-fs/file-identity schema-directory)) "reopen kept it")
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"fresh storage epoch"
              (store/open! (assoc opts :initialize? true))))
        (is (= identity (exclusive-fs/file-identity schema-directory)) "initialize kept it")
        (is (not (fs/exists? ledger-file))))
      (finally (fs/remove-tree! root)))))

(deftest recovery-cleanup-failure-keeps-the-marker-test
  "If recovery cannot remove an owned artifact, it fails without clearing the
   marker, so the next start can still recover."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        original-delete fs/delete-if-exists!
        original-unlink exclusive-fs/unlink-if-empty!]
    (try
      (fs/ensure-dir! (path/dirname ledger-file))
      (fs/write-text! ledger-file "")
      (fs/ensure-dir! schema-directory)
      (fs/write-text! (init-marker-path schema-directory)
                      (str (pr-str {:sol.clio/init-pending true
                                    :phase :publishing
                                    :ledger-file ledger-file
                                    :schema-directory schema-directory
                                    :schema/root "deadroot"
                                    :ledger-identity (exclusive-fs/file-identity ledger-file)})
                           "\n"))
      (with-redefs [fs/delete-if-exists! (fn [p]
                                           (if (= p ledger-file)
                                             (throw (ex-info "unlink failed" {}))
                                             (original-delete p)))
                    exclusive-fs/unlink-if-empty! (fn [p]
                                                    (if (= p ledger-file)
                                                      (throw (ex-info "unlink failed" {}))
                                                      (original-unlink p)))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"unlink failed"
              (store/open! (assoc opts :initialize? true)))))
      (is (fs/exists? (init-marker-path schema-directory)) "recovery evidence kept")
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store)) "next start recovers"))
      (finally (fs/remove-tree! root)))))

(deftest schema-publish-failure-never-unlinks-a-ledger-another-process-appended-test
  ;; The ledger is visible between its link and the schema link; an append by
  ;; another opener in that window must survive. Staging + marker stay so
  ;; recovery can resume the schema publish against the retained ledger.
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        accepted "{:accepted/by \"other opener\"}\n"
        original-link exclusive-fs/link-new!]
    (try
      (with-redefs [exclusive-fs/link-new!
                    (fn [from to]
                      (if (= (path/resolve schema-directory) (path/dirname (path/resolve to)))
                        (do (fs/append-text! ledger-file accepted)
                            (throw (ex-info "schema publish failed" {})))
                        (original-link from to)))]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"schema publish failed"
              (store/open! (assoc opts :initialize? true)))))
      (is (and (fs/exists? ledger-file) (= accepted (fs/read-text ledger-file)))
          "the other process's accepted history is kept")
      (is (fs/exists? (init-marker-path schema-directory))
          "marker retained so recovery can resume against the kept ledger")
      (is (seq (schema-staging-leftovers schema-directory))
          "schema staging retained for resume")
      (finally (fs/remove-tree! root)))))

(deftest unlink-if-empty-keeps-ledger-while-append-lock-held-test
  "Exclusive append lock must prove no writer can append after the emptiness
   check; while the lock is held, unlink-if-empty! leaves the ledger in place."
  (let [root (scratch)
        ledger (path/join root "events.edn")
        lock-path (exclusive-fs/append-lock-path ledger)]
    (try
      (fs/ensure-dir! root)
      (fs/write-text! ledger "")
      (let [lock (exclusive-fs/lock-file! lock-path)]
        (try
          (is (= :busy (exclusive-fs/unlink-if-empty! ledger))
              "must not destroy an empty ledger while a writer holds the append lock")
          (is (fs/exists? ledger) "ledger path kept")
          (is (= "" (fs/read-text ledger)))
          (finally (exclusive-fs/release-lock! lock))))
      (finally (fs/remove-tree! root)))))

(deftest rollback-rmdir-failure-keeps-recovery-evidence-test
  "If directory removal fails after the marker is cleared, recovery evidence
   (claim) must remain so the next start can finish the rollback."
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        original-rmdir exclusive-fs/remove-directory-if-empty!]
    (try
      (fs/ensure-dir! (path/dirname ledger-file))
      (fs/write-text! ledger-file "")
      (fs/ensure-dir! schema-directory)
      (fs/write-text! (init-marker-path schema-directory)
                      (str (pr-str {:sol.clio/init-pending true
                                    :phase :publishing
                                    :ledger-file ledger-file
                                    :schema-directory schema-directory
                                    :schema/root "deadroot"
                                    :ledger-identity (exclusive-fs/file-identity ledger-file)})
                           "\n"))
      (with-redefs [exclusive-fs/remove-directory-if-empty!
                    (fn [p]
                      (if (= p schema-directory)
                        (throw (js/Error. "rmdir busy"))
                        (original-rmdir p)))]
        (is (thrown-with-msg? js/Error #"rmdir busy"
              (store/open! (assoc opts :initialize? true)))))
      (is (fs/exists? schema-directory) "schema directory still present")
      (is (fs/exists? (str (path/resolve schema-directory) ".sol-clio-claim"))
          "claim evidence kept for the next start")
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store)) "next start recovers"))
      (finally (fs/remove-tree! root)))))

(deftest claim-tmp-ownership-record-recovers-markerless-empty-directory-test
  ;; Crash after claim body fsync but before replace-file! rename leaves only
  ;; `.sol-clio-claim.tmp`; recovery must accept that temporary ownership
  ;; record and release the empty directory.
  (let [root (scratch)
        {:keys [schema-directory] :as opts} (options root)
        claim (str (path/resolve schema-directory) ".sol-clio-claim")
        claim-tmp (str claim ".tmp")]
    (try
      (fs/ensure-dir! schema-directory)
      (fs/write-text! claim-tmp
                      (str (pr-str {:sol.clio/claim (path/resolve schema-directory)
                                    :identity (exclusive-fs/file-identity schema-directory)})
                           "\n"))
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store)) "tmp claim recovered; fresh epoch")
        (is (not (fs/exists? claim-tmp)))
        (is (not (fs/exists? claim))))
      (finally (fs/remove-tree! root)))))

(deftest unlink-if-empty-leaves-nonempty-ledger-at-public-path-test
  ;; A nonempty ledger must never leave its public path during unpublish.
  (let [root (scratch)
        ledger (path/join root "events.edn")
        body "{:kept true}\n"]
    (try
      (fs/ensure-dir! root)
      (fs/write-text! ledger body)
      (is (= :kept (exclusive-fs/unlink-if-empty! ledger)))
      (is (fs/exists? ledger) "public path retained")
      (is (= body (fs/read-text ledger)))
      (is (= [] (filterv #(str/starts-with? % "events.edn.unpublish-")
                         (fs/list-files root)))
          "no aside hold left behind")
      (finally (fs/remove-tree! root)))))

(deftest recovery-marker-matches-trailing-separator-path-spellings-test
  ;; Marker paths recorded with a trailing separator still match on recovery.
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)]
    (try
      (fs/ensure-dir! (path/dirname ledger-file))
      (fs/write-text! ledger-file "")
      (fs/ensure-dir! schema-directory)
      (let [identity (exclusive-fs/file-identity ledger-file)]
        (fs/write-text! (init-marker-path schema-directory)
                        (str (pr-str {:sol.clio/init-pending true
                                      :phase :publishing
                                      :ledger-file (str (path/resolve ledger-file) "/")
                                      :schema-directory (str (path/resolve schema-directory) "/")
                                      :schema/root "deadroot"
                                      :ledger-identity identity})
                             "\n")))
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store))
            "trailing-separator marker paths recovered")
        (is (not (fs/exists? (init-marker-path schema-directory)))))
      (finally (fs/remove-tree! root)))))

(deftest stale-claim-evidence-never-adopts-a-directory-sol-did-not-create-test
  ;; Claim evidence must name the identity of the directory Sol created; a
  ;; stale claim beside a foreign empty directory proves nothing.
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)]
    (try
      (fs/ensure-dir! schema-directory)
      (fs/write-text! (str (path/resolve schema-directory) ".sol-clio-claim")
                      (str (pr-str {:sol.clio/claim (path/resolve schema-directory)}) "\n"))
      (let [identity (exclusive-fs/file-identity schema-directory)]
        (is (thrown-with-msg? cljs.core/ExceptionInfo #"fresh storage epoch"
              (store/open! (assoc opts :initialize? true))))
        (is (= identity (exclusive-fs/file-identity schema-directory)) "foreign directory kept")
        (is (not (fs/exists? ledger-file))))
      (finally (fs/remove-tree! root)))))
