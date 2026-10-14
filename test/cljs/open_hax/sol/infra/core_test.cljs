(ns open-hax.sol.infra.core-test
  (:require [cljs.test :refer [deftest is]]
            [clio.extern.js.fs :as fs]
            [open-hax.sol.domain.node.path :as path]
            [open-hax.sol.infra.agent.clio-store :as clio-store]
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
