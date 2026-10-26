#!/usr/bin/env bb
;; Exercises the actual planning-check CLI. Fixtures never alter source or board state.
(ns check-review-worker-plan-test
  (:require [babashka.process :as process]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is run-tests]]))

(def original-plan
  (edn/read-string (slurp "docs/design/persistent-review-workers.edn")))

(defn check-fixture
  ([plan] (check-fixture plan nil))
  ([plan source-root]
   (let [directory (io/file ".ημ" "sol-review-plan" "fixtures")
         _ (.mkdirs directory)
         file (io/file directory (str (random-uuid) ".edn"))]
     (try
       (spit file (pr-str plan))
       (apply process/shell {:out :string :err :string :continue true}
              "bb" "scripts/check-review-worker-plan.clj" (str file)
              (when source-root [(str source-root)]))
       (finally (.delete file))))))

(defn copied-source-root
  "Copy every integration file into a disposable root so a fixture can edit
   one source without touching the real tree."
  [edit-path edit-fn]
  (let [root (io/file ".ημ" "sol-review-plan" "sources" (str (random-uuid)))]
    (doseq [{:keys [path]} (:integration/points original-plan)]
      (let [target (io/file root path)]
        (io/make-parents target)
        (spit target (cond-> (slurp path) (= path edit-path) edit-fn))))
    root))

(defn delete-tree [file]
  (doseq [f (reverse (file-seq (io/file file)))] (.delete f)))

(def service-path "src/cljs/open_hax/sol/infra/agent/service.cljs")

(defn without-service-anchor-code
  "Remove every code occurrence of IAgentService, keeping it only in a
   comment and a string."
  [source]
  (-> source
      (str/replace "(defprotocol IAgentService" ";; IAgentService was here\n(def removed-protocol \"IAgentService\")\n(defprotocol RemovedService")
      (str/replace #"(?m)^(\s+)IAgentService\s*$" "$1RemovedService")))

(defn with-service-anchor-discarded
  "Keep IAgentService only inside a `#_` discarded form, which the reader
   drops."
  [source]
  (-> source
      (str/replace "(defprotocol IAgentService" "#_(defprotocol IAgentService)\n(defprotocol RemovedService")
      (str/replace #"(?m)^(\s+)IAgentService\s*$" "$1RemovedService")))

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

(deftest copied-source-root-passes
  (let [root (copied-source-root service-path identity)]
    (try
      (is (zero? (:exit (check-fixture original-plan root))))
      (finally (delete-tree root)))))

(deftest anchor-only-in-comment-or-string-is-rejected
  (let [root (copied-source-root service-path without-service-anchor-code)]
    (try
      (let [edited (slurp (io/file root service-path))]
        (is (str/includes? edited ";; IAgentService"))
        (is (str/includes? edited "\"IAgentService\""))
        (is (not (str/includes? edited "defprotocol IAgentService"))))
      (let [result (check-fixture original-plan root)]
        (is (= 1 (:exit result)))
        (is (re-find #"Anchor drift" (:err result))))
      (finally (delete-tree root)))))

(deftest anchor-only-in-discarded-form-is-rejected
  (let [root (copied-source-root service-path with-service-anchor-discarded)]
    (try
      (let [edited (slurp (io/file root service-path))]
        (is (str/includes? edited "#_(defprotocol IAgentService)"))
        (is (= 1 (count (re-seq #"IAgentService" edited)))))
      (let [result (check-fixture original-plan root)]
        (is (= 1 (:exit result)))
        (is (re-find #"Anchor drift" (:err result))))
      (finally (delete-tree root)))))

(let [{:keys [fail error]} (run-tests)]
  (System/exit (if (zero? (+ fail error)) 0 1)))
