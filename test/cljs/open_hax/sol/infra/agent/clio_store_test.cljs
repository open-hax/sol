(ns open-hax.sol.infra.agent.clio-store-test
  (:require [cljs.test :refer [deftest is]]
            [clio.extern.js.fs :as fs]
            [clio.infra.event :as event]
            [clio.infra.ledger :as ledger]
            [clio.infra.runtime :as runtime]
            [open-hax.sol.clio-native-probe :as probe]
            [open-hax.sol.domain.node.path :as path]
            [open-hax.sol.extern.exclusive-fs :as exclusive-fs]
            [open-hax.sol.infra.agent.clio-store :as store]
            [open-hax.sol.infra.agent.episode-ledger :as episode]
            [open-hax.sol.infra.agent.session-store :as sessions]
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
(defn error-kind [f]
  (try (f) nil (catch :default error (:clio/error (ex-data error)))))
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
                      (when (= directory schema-directory)
                        (original-open directory catalog)
                        (fs/ensure-dir! nested)
                        (fs/write-text! nested-revision other)
                        (fs/write-text! foreign-ledger foreign)
                        (throw (ex-info "schema materialization failed" {})))
                      (original-open directory catalog))]
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
      (is (not (fs/exists? schema-directory)))
      (let [canonical-store (store/open! (assoc opts :initialize? true))]
        (is (= [] (store/events canonical-store)) "retry still sees a fresh storage epoch"))
      (finally (fs/remove-tree! root)))))

(deftest exclusive-open-failure-never-deletes-another-attempts-ledger-test
  (let [root (scratch)
        {:keys [ledger-file] :as opts} (options root)
        other "{:written/by \"another attempt\"}\n"]
    (try
      (with-redefs [exclusive-fs/create-file!
                    (fn [p _on-created]
                      ;; Another attempt won the race between the fresh-epoch
                      ;; check and the exclusive open.
                      (fs/write-text! p other)
                      (throw (js/Error. "EEXIST: file already exists")))]
        (is (thrown-with-msg? js/Error #"EEXIST"
              (store/open! (assoc opts :initialize? true)))))
      (is (= other (fs/read-text ledger-file)) "the other attempt's ledger is kept")
      (finally (fs/remove-tree! root)))))

(deftest failed-initialization-keeps-a-schema-directory-another-attempt-claimed-test
  (let [root (scratch)
        {:keys [ledger-file schema-directory] :as opts} (options root)
        other-revision (path/join schema-directory "other.edn")
        other "{:written/by \"another initializer\"}\n"]
    (try
      (with-redefs [exclusive-fs/create-directory!
                    (fn [p _on-created]
                      ;; Another initializer claimed the same schema directory
                      ;; between this attempt's fresh-epoch check and its
                      ;; exclusive mkdir. We must not delete their directory.
                      (fs/ensure-dir! p)
                      (fs/write-text! other-revision other)
                      (throw (js/Error. "EEXIST: directory already exists")))]
        (is (thrown-with-msg? js/Error #"EEXIST"
              (store/open! (assoc opts :initialize? true)))))
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
        (with-redefs [runtime/open (fn [_ _] (throw (ex-info "reopen failed" {})))]
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
