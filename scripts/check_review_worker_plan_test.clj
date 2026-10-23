#!/usr/bin/env bb
;; Exercises the actual planning-check CLI. Fixtures never alter source or board state.
(ns check-review-worker-plan-test
  (:require [babashka.process :as process]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is run-tests]]))

(def original-plan
  (edn/read-string (slurp "docs/design/persistent-review-workers.edn")))

(defn check-fixture [plan]
  (let [directory (io/file ".ημ" "sol-review-plan" "fixtures")
        _ (.mkdirs directory)
        file (io/file directory (str (random-uuid) ".edn"))]
    (try
      (spit file (pr-str plan))
      (process/shell {:out :string :err :string :continue true}
                     "bb" "scripts/check-review-worker-plan.clj" (str file))
      (finally (.delete file)))))

(deftest complete-inventory-passes
  (is (zero? (:exit (check-fixture original-plan)))))

(deftest dependency-removal-is-not-a-pass
  (let [result (check-fixture (update original-plan :dependencies dissoc 'open-hax/eta-mu))]
    (is (= 1 (:exit result)))
    (is (re-find #"dependency inventory" (:err result)))))

(deftest integration-removal-is-not-a-pass
  (let [result (check-fixture (update original-plan :integration/points #(vec (rest %))))]
    (is (= 1 (:exit result)))
    (is (re-find #"integration inventory" (:err result)))))

(deftest extra-dependency-is-rejected
  (let [result (check-fixture (assoc-in original-plan [:dependencies 'unknown/package]
                                      {:git/sha (:source/revision original-plan)}))]
    (is (= 1 (:exit result)))
    (is (re-find #"dependency inventory" (:err result)))))

(deftest extra-existing-integration-file-is-rejected
  (let [extra {:path "src/cljs/open_hax/sol/shape/agent.cljs"
               :namespace "open-hax.sol.shape.agent" :anchor "IAgentSession"}
        result (check-fixture (update original-plan :integration/points conj extra))]
    (is (= 1 (:exit result)))
    (is (re-find #"integration inventory" (:err result)))))

(deftest removing-pin-fields-is-rejected
  (let [result (check-fixture (update-in original-plan [:dependencies 'open-hax/eta-mu]
                                       dissoc :git/sha))]
    (is (= 1 (:exit result)))
    (is (re-find #"Dependency pin drift" (:err result)))))

(deftest duplicate-integration-is-rejected
  (let [result (check-fixture (update original-plan :integration/points conj
                                       (first (:integration/points original-plan))))]
    (is (= 1 (:exit result)))
    (is (re-find #"Duplicate source paths" (:err result)))))

(deftest service-activation-is-rejected
  (let [result (check-fixture (assoc original-plan :activation/enabled? true))]
    (is (= 1 (:exit result)))
    (is (re-find #"must not activate" (:err result)))))

(deftest missing-integration-gap-is-rejected
  (let [result (check-fixture (update-in original-plan [:integration/points 0] dissoc :gap))]
    (is (= 1 (:exit result)))
    (is (re-find #"Missing integration gap" (:err result)))))

(deftest non-keyword-integration-gap-is-rejected
  (let [result (check-fixture (assoc-in original-plan [:integration/points 0 :gap] "durable-review-job-delegate"))]
    (is (= 1 (:exit result)))
    (is (re-find #"Missing integration gap" (:err result)))))

(deftest wrong-integration-anchor-is-rejected
  (let [result (check-fixture (assoc-in original-plan [:integration/points 0 :anchor] "ns"))]
    (is (= 1 (:exit result)))
    (is (re-find #"Unexpected integration anchor" (:err result)))))

(let [{:keys [fail error]} (run-tests)]
  (System/exit (if (zero? (+ fail error)) 0 1)))
