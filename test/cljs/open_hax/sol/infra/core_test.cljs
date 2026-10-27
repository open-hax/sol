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
  (let [root (scratch)]
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
      (finally (fs/remove-tree! root)))))

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
        saved [@session-store/default-store* @run-state/default-store*]]
    (try
      (session-store/set-default-store! nil)
      (run-state/set-default-store! nil)
      (let [resolved (register-with-stub-routes! (clio-config root))
            sessions @session-store/default-store*]
        (is (some? sessions) "session routes have a default store")
        (is (identical? (:clio/store resolved) (:canonical-store sessions))
            "the session store writes through the resolved Clio store")
        (is (some? @run-state/default-store*) "run routes have a default store"))
      (finally
        (session-store/set-default-store! (first saved))
        (run-state/set-default-store! (second saved))
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

(deftest retry-after-failed-registration-follows-the-resolved-clio-store-test
  ;; A retry after a failed register-app-routes! call must not keep a session
  ;; store that this path created for a different resolved Clio store.
  (let [root-a (scratch)
        root-b (scratch)
        saved [@session-store/default-store* @run-state/default-store* @core/auto-session-store*]]
    (try
      (reset-stores!)
      (register-failing! (clio-config root-a))
      (let [runs @run-state/default-store*
            resolved (register-with-stub-routes! (clio-config root-b))
            sessions @session-store/default-store*]
        (is (identical? (:clio/store resolved) (:canonical-store sessions))
            "the auto-installed session store follows the retried Clio store")
        (is (identical? runs @run-state/default-store*)
            "the run store has no Clio binding and is kept"))
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
