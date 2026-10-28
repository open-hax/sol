(ns open-hax.sol.infra.core-test
  (:require [cljs.test :refer [deftest is]]
            [clio.extern.js.fs :as fs]
            [open-hax.sol.domain.node.path :as path]
            [open-hax.sol.infra.agent.clio-store :as clio-store]
            [open-hax.sol.infra.agent.run-state :as run-state]
            [open-hax.sol.infra.agent.session-store :as session-store]
            [open-hax.sol.infra.core :as core]
            [open-hax.sol.infra.routes.app :as app-routes]
            [open-hax.sol.runtime.state :as runtime-state]))

(defn- scratch [] (path/resolve ".ημ" "sol-core-tests" (str (random-uuid))))

(defn- clio-config [root]
  {:clio/ledger-file (path/join root "epoch" "events.edn")
   :clio/schema-directory (path/join root "epoch" "schemas")
   :clio/initialize? true})

(deftest exported-seams-open-the-configured-clio-store-test
  ;; start! and solPlugin both resolve through this function; neither goes
  ;; through bootstrap!, so the store must be opened here.
  (let [root (scratch)
        saved-stores [@session-store/default-store* @run-state/default-store* @core/auto-session-store*]]
    (try
      ;; Exercise the caller boundary: if register-app-routes! stopped resolving
      ;; through resolve-app-config, routes would receive no :clio/store.
      (let [captured (atom nil)
            saved [@runtime-state/config* @runtime-state/runtime*]]
        (try
          (with-redefs [app-routes/register-routes!
                        (fn [_runtime _app config] (reset! captured config))]
            (core/register-app-routes! #js {} #js {} (clio-config root)))
          (finally
            (reset! runtime-state/config* (first saved))
            (reset! runtime-state/runtime* (second saved))))
        (let [resolved @captured]
          (is (some? (:clio/store resolved)) "Clio paths reach routes with an open store")
          (is (= [] (clio-store/events (:clio/store resolved))))))
      (finally
        ;; Registration installs process-wide defaults; restore all three
        ;; before the Clio root they point at is deleted.
        (session-store/set-default-store! (nth saved-stores 0))
        (run-state/set-default-store! (nth saved-stores 1))
        (reset! core/auto-session-store* (nth saved-stores 2))
        (fs/remove-tree! root)))))

(deftest already-open-store-is-not-reopened-test
  (let [root (scratch)]
    (try
      (let [first-pass (core/resolve-app-config (clio-config root))
            second-pass (core/resolve-app-config first-pass)]
        (is (identical? (:clio/store first-pass) (:clio/store second-pass))
            "bootstrap!'s store is reused, never re-initialized"))
      (finally (fs/remove-tree! root)))))

(deftest no-clio-paths-means-no-store-test
  (is (nil? (:clio/store (core/resolve-app-config {:host "127.0.0.1" :port 0})))))

(defn- register-with-stub-routes! [config]
  (let [saved [@runtime-state/config* @runtime-state/runtime*]]
    (try
      (with-redefs [app-routes/register-routes! (fn [_runtime _app resolved] resolved)]
        (core/register-app-routes! #js {} #js {} config))
      (finally
        (reset! runtime-state/config* (first saved))
        (reset! runtime-state/runtime* (second saved))))))

(deftest exported-seams-install-default-stores-test
  ;; start! and solPlugin skip bootstrap!, so session and run routes would
  ;; throw "No default Sol session store configured" unless the shared
  ;; startup path installs the default stores.
  (let [root (scratch)
        saved [@session-store/default-store* @run-state/default-store* @core/auto-session-store*]]
    (try
      (session-store/set-default-store! nil)
      (run-state/set-default-store! nil)
      (reset! core/auto-session-store* nil)
      (let [resolved (register-with-stub-routes! (clio-config root))
            sessions @session-store/default-store*]
        (is (some? sessions) "session routes have a default store")
        (is (identical? (:clio/store resolved) (:canonical-store sessions))
            "the session store writes through the resolved Clio store")
        (is (some? @run-state/default-store*) "run routes have a default store"))
      (finally
        (session-store/set-default-store! (first saved))
        (run-state/set-default-store! (second saved))
        (reset! core/auto-session-store* (nth saved 2))
        (fs/remove-tree! root)))))

(deftest installed-default-stores-are-kept-test
  ;; bootstrap! installs its stores first; the shared path must not replace them.
  (let [saved [@session-store/default-store* @run-state/default-store*]
        sessions (session-store/create-edn-session-store (scratch))
        runs (run-state/create-edn-run-state-store (scratch))]
    (try
      (session-store/set-default-store! sessions)
      (run-state/set-default-store! runs)
      (register-with-stub-routes! {:host "127.0.0.1" :port 0})
      (is (identical? sessions @session-store/default-store*))
      (is (identical? runs @run-state/default-store*))
      (finally
        (session-store/set-default-store! (first saved))
        (run-state/set-default-store! (second saved))))))

(defn- register-failing! [config]
  (let [saved [@runtime-state/config* @runtime-state/runtime*]]
    (try
      (with-redefs [app-routes/register-routes!
                    (fn [_runtime _app _resolved] (throw (ex-info "route registration failed" {})))]
        (core/register-app-routes! #js {} #js {} config))
      (catch :default _ nil)
      (finally
        (reset! runtime-state/config* (first saved))
        (reset! runtime-state/runtime* (second saved))))))

(defn- reset-stores! []
  (reset! core/auto-session-store* nil)
  (session-store/set-default-store! nil)
  (run-state/set-default-store! nil))

(deftest failed-route-registration-pins-the-session-store-test
  ;; register-routes! can attach some Sol routes to the caller's app and then
  ;; throw (for example on a conflicting route in an existing Fastify app).
  ;; Those handlers already use the auto-installed session store, so a retry
  ;; naming a different Clio ledger must be rejected rather than rebinding
  ;; the process-wide store under them. A retry with the same ledger works.
  (let [root-a (scratch)
        root-b (scratch)
        saved [@session-store/default-store* @run-state/default-store* @core/auto-session-store*]]
    (try
      (reset-stores!)
      (register-failing! (clio-config root-a))
      (let [sessions @session-store/default-store*
            runs @run-state/default-store*
            rejected (try (register-with-stub-routes! (clio-config root-b))
                          nil
                          (catch :default e e))]
        (is (= :sol/incompatible-clio-store (:type (ex-data rejected)))
            "a different ledger after a partial registration is rejected")
        (is (identical? sessions @session-store/default-store*)
            "the partially registered app's session store is kept")
        (is (identical? runs @run-state/default-store*)
            "the run store has no Clio binding and is kept")
        (let [again (register-with-stub-routes! (assoc (clio-config root-a) :clio/initialize? false))]
          (is (identical? (:canonical-store sessions) (:clio/store again))
              "a retry with the same ledger reuses the session store's Clio store")
          (is (identical? sessions @session-store/default-store*))))
      (finally
        (session-store/set-default-store! (first saved))
        (run-state/set-default-store! (second saved))
        (reset! core/auto-session-store* (nth saved 2))
        (fs/remove-tree! root-a)
        (fs/remove-tree! root-b)))))

(deftest active-app-store-is-not-rebound-test
  ;; Once an app registered successfully, its routes write through the session
  ;; store's Clio ledger. A second registration with a different Clio store
  ;; must be rejected before any shared state changes.
  (let [root-a (scratch)
        root-b (scratch)
        saved [@session-store/default-store* @run-state/default-store* @core/auto-session-store*]]
    (try
      (reset-stores!)
      (let [first-resolved (register-with-stub-routes! (clio-config root-a))
            sessions @session-store/default-store*
            config-before @runtime-state/config*
            ;; Call register-app-routes! directly so config* is read right
            ;; after the rejection, before any helper cleanup restores it.
            [rejected config-after]
            (with-redefs [app-routes/register-routes! (fn [_runtime _app resolved] resolved)]
              (try (core/register-app-routes! #js {} #js {} (clio-config root-b))
                   [nil @runtime-state/config*]
                   (catch :default e [e @runtime-state/config*])))]
        (is (some? rejected) "an incompatible second registration is rejected")
        (is (identical? sessions @session-store/default-store*)
            "the running app's session store is kept")
        (is (identical? (:clio/store first-resolved) (:canonical-store @session-store/default-store*)))
        (is (identical? config-before config-after)
            "the rejected registration leaves config* untouched")
        (register-with-stub-routes! (assoc (clio-config root-a) :clio/store (:clio/store first-resolved)))
        (is (identical? sessions @session-store/default-store*)
            "re-registering with the same Clio store is allowed and keeps the store"))
      (finally
        (session-store/set-default-store! (first saved))
        (run-state/set-default-store! (second saved))
        (reset! core/auto-session-store* (nth saved 2))
        (fs/remove-tree! root-a)
        (fs/remove-tree! root-b)))))

(deftest same-paths-re-registration-reuses-the-active-store-test
  ;; start! retried after a failed .listen, or a second solPlugin call,
  ;; resolves the same SOL_CLIO_* paths into a freshly opened store object.
  ;; That is the same ledger, so it must not be rejected; routes reuse the
  ;; store the running app's session store writes through.
  (let [root (scratch)
        saved [@session-store/default-store* @run-state/default-store* @core/auto-session-store*]]
    (try
      (reset-stores!)
      (let [first-resolved (register-with-stub-routes! (clio-config root))
            sessions @session-store/default-store*
            again (register-with-stub-routes! (assoc (clio-config root) :clio/initialize? false))]
        (is (identical? sessions @session-store/default-store*)
            "the running app's session store is kept")
        (is (identical? (:clio/store first-resolved) (:clio/store again))
            "routes reuse the active Clio store for the same paths"))
      (finally
        (session-store/set-default-store! (first saved))
        (run-state/set-default-store! (second saved))
        (reset! core/auto-session-store* (nth saved 2))
        (fs/remove-tree! root)))))

(deftest aliased-paths-re-registration-uses-the-active-store-paths-test
  ;; A relative alias of the running app's SOL_CLIO_* paths names the same
  ;; ledger, so the active store is reused. The returned config must then
  ;; carry that store's own paths too: resolving it again (hot reload, a
  ;; later start) compares path strings and would otherwise demand a restart.
  (let [root (scratch)
        saved [@session-store/default-store* @run-state/default-store* @core/auto-session-store*]]
    (try
      (reset-stores!)
      (let [first-resolved (register-with-stub-routes! (clio-config root))
            bound (:clio/store first-resolved)
            alias (-> (clio-config root)
                      (update :clio/ledger-file #(path/relative (path/cwd) %))
                      (update :clio/schema-directory #(path/relative (path/cwd) %))
                      (assoc :clio/initialize? false))
            again (register-with-stub-routes! alias)]
        (is (not= (:clio/ledger-file alias) (:ledger-file bound))
            "the second registration spells the ledger path differently")
        (is (identical? bound (:clio/store again))
            "routes reuse the active Clio store for the aliased paths")
        (is (= (:ledger-file bound) (:clio/ledger-file again))
            "the returned config names the reused store's ledger file")
        (is (= (:clio/schema-directory first-resolved) (:clio/schema-directory again))
            "the returned config names the reused store's schema directory")
        (is (= (:clio/ledger-file again)
               (:clio/ledger-file (core/resolve-app-config again)))
            "resolving the returned config again keeps the reused store"))
      (finally
        (session-store/set-default-store! (first saved))
        (run-state/set-default-store! (second saved))
        (reset! core/auto-session-store* (nth saved 2))
        (fs/remove-tree! root)))))

(deftest ^:async rejected-start-leaves-runtime-state-untouched-test
  ;; start! must not write config*/runtime* before register-app-routes! has
  ;; checked store compatibility; otherwise a rejected start! leaves config*
  ;; describing a Clio store no running app uses.
  (let [root-a (scratch)
        root-b (scratch)
        resolve-original core/resolve-app-config
        register-original app-routes/register-routes!
        saved [@session-store/default-store* @run-state/default-store* @core/auto-session-store*
               @runtime-state/config* @runtime-state/runtime*]]
    (try
      (reset-stores!)
      (register-with-stub-routes! (clio-config root-a))
      (let [config-before @runtime-state/config*
            runtime-before @runtime-state/runtime*
            ;; start! is async, so the stubs stay installed until it settles.
            _ (set! core/resolve-app-config (fn [_] (resolve-original (clio-config root-b))))
            _ (set! app-routes/register-routes! (fn [_runtime _app resolved] resolved))
            rejected (try (await (core/start! #js {:marker "rejected"}))
                          nil
                          (catch :default e e))]
        (is (some? rejected) "start! with a different Clio ledger is rejected")
        (is (identical? config-before @runtime-state/config*)
            "a rejected start! leaves config* untouched")
        (is (identical? runtime-before @runtime-state/runtime*)
            "a rejected start! leaves runtime* untouched"))
      (finally
        (set! core/resolve-app-config resolve-original)
        (set! app-routes/register-routes! register-original)
        (session-store/set-default-store! (nth saved 0))
        (run-state/set-default-store! (nth saved 1))
        (reset! core/auto-session-store* (nth saved 2))
        (reset! runtime-state/config* (nth saved 3))
        (reset! runtime-state/runtime* (nth saved 4))
        (fs/remove-tree! root-a)
        (fs/remove-tree! root-b)))))

(deftest bootstrap-keeps-the-active-auto-session-store-test
  ;; bootstrap is a public export, so a host can call it after solPlugin
  ;; registered routes. Those routes resolve the default session store at
  ;; operation time, so bootstrap! must not install a fresh store over the
  ;; active auto-installed one: the same ledger reuses it, and a different
  ;; ledger is rejected. With no active auto store, bootstrap! installs its
  ;; own store as before.
  (let [root-a (scratch)
        root-b (scratch)
        base-dir (scratch)
        saved [@session-store/default-store* @run-state/default-store* @core/auto-session-store*]]
    (try
      (reset-stores!)
      (let [fresh (core/bootstrap-session-store!
                   base-dir (clio-store/configure! (clio-config root-a)))]
        (is (some? fresh) "with no active auto store bootstrap! gets a new store")
        (is (nil? @session-store/default-store*)
            "choosing the bootstrap store does not install it"))
      (reset-stores!)
      (register-with-stub-routes! (clio-config root-a))
      (let [sessions @session-store/default-store*
            same-paths (clio-store/configure! (assoc (clio-config root-a) :clio/initialize? false))
            reused (core/bootstrap-session-store! base-dir same-paths)
            rejected (try (core/bootstrap-session-store!
                           base-dir (clio-store/configure! (clio-config root-b)))
                          nil
                          (catch :default e e))]
        (is (identical? sessions reused)
            "bootstrap! with the same ledger reuses the active session store")
        (is (= :sol/incompatible-clio-store (:type (ex-data rejected)))
            "bootstrap! with a different ledger is rejected while an app runs")
        (is (identical? sessions @session-store/default-store*)
            "the running app's session store is kept"))
      (finally
        (session-store/set-default-store! (first saved))
        (run-state/set-default-store! (second saved))
        (reset! core/auto-session-store* (nth saved 2))
        (fs/remove-tree! root-a)
        (fs/remove-tree! root-b)
        (fs/remove-tree! base-dir)))))
