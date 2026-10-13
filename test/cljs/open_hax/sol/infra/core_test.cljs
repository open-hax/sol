(ns open-hax.sol.infra.core-test
  (:require [cljs.test :refer [deftest is]]
            [clio.extern.js.fs :as fs]
            [open-hax.sol.domain.node.path :as path]
            [open-hax.sol.infra.agent.clio-store :as clio-store]
            [open-hax.sol.infra.core :as core]))

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
      (let [resolved (core/resolve-app-config (clio-config root))]
        (is (some? (:clio/store resolved)) "Clio paths resolve to an open store")
        (is (= [] (clio-store/events (:clio/store resolved)))))
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
