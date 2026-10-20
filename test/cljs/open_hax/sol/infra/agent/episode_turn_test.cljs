(ns open-hax.sol.infra.agent.episode-turn-test
  (:require [cljs.test :refer [deftest is]]
            [open-hax.sol.infra.agent.episode-turn :as episode-turn]))

(defn- sequential-id-fn
  [ids]
  (let [remaining* (atom (vec ids))]
    (fn []
      (let [id (first @remaining*)]
        (swap! remaining* subvec 1)
        id))))

(def turn-request
  {:run-id "run-1"
   :session-id "session-1"
   :conversation-id "conversation-1"
   :model "model-1"
   :message "hello"
   :agent-spec {:actor-id "actor.agent.research"
                :contract-id "agent/research"
                :contract-revision "git:abc123"}
   :auth-context {:actorId "actor.agent.research"
                  :entityId "entity.agent.research"
                  :orgId "org.open-hax"
                  :principalKind "agent"}})

(deftest ^:async successful-turn-emits-complete-episode-test
  (let [events* (atom [])
        executed-request* (atom nil)
        expected-result {:answer "done"
                         :run_id "run-1"
                         :session_id "session-1"
                         :conversation_id "conversation-1"
                         :model "model-1"}
        config {:sol-node-id "sol.node.local"
                :clio/correlation-id-fn
                (sequential-id-fn
                 ["turn-1" "episode-1"
                  "event-1" "event-2" "event-3" "event-4"])
                :clio/append!
                (fn [envelope]
                  (swap! events* conj envelope)
                  (js/Promise.resolve :appended))
                :turn-executor!
                (fn [_runtime _config request]
                  (reset! executed-request* request)
                  (js/Promise.resolve expected-result))}
        actual (await (episode-turn/send-agent-turn!
                       {:runtime true}
                       config
                       turn-request))]
    (is (= expected-result actual))
    (is (= turn-request @executed-request*))
    (is (= [:sol.run/started
            :sol.turn/started
            :sol.turn/completed
            :sol.run/completed]
           (mapv :event/type @events*)))
    (is (= (into [nil] (butlast (mapv :event/id @events*)))
           (mapv #(first (:event/causes %)) @events*)))
    (is (= #{(:event/id (first @events*))}
           (set (map #(get-in % [:event/data :causal/root]) @events*))))
    (is (= "completed"
           (get-in (last @events*) [:event/data :payload :status])))
    (is (= "model-1"
           (get-in (last @events*) [:event/data :payload :model])))))

(deftest ^:async failed-turn-emits-failure-terminal-test
  (let [events* (atom [])
        turn-error (js/Error. "provider failed")
        config {:clio/correlation-id-fn
                (sequential-id-fn
                 ["turn-1" "episode-1"
                  "event-1" "event-2" "event-3" "event-4"])
                :clio/append!
                (fn [envelope]
                  (swap! events* conj envelope)
                  (js/Promise.resolve :appended))
                :turn-executor!
                (fn [_runtime _config _request]
                  (js/Promise.reject turn-error))}]
    (try
      (await (episode-turn/send-agent-turn! nil config turn-request))
      (is false "turn should reject")
      (catch :default actual-error
        (is (identical? turn-error actual-error))))
    (is (= [:sol.run/started
            :sol.turn/started
            :sol.turn/failed
            :sol.run/failed]
           (mapv :event/type @events*)))
    (is (= "provider failed"
           (get-in (last @events*) [:event/data :payload :error])))
    (is (= (into [nil] (butlast (mapv :event/id @events*)))
           (mapv #(first (:event/causes %)) @events*)))))

(deftest ^:async no-appender-preserves-local-runtime-test
  (let [executed?* (atom false)
        expected-result {:answer "local" :model "model-1"}
        config {:clio/correlation-id-fn
                (sequential-id-fn
                 ["turn-1" "episode-1"
                  "event-1" "event-2" "event-3" "event-4"])
                :turn-executor!
                (fn [_runtime _config _request]
                  (reset! executed?* true)
                  (js/Promise.resolve expected-result))}
        result (await (episode-turn/send-agent-turn! nil config turn-request))]
    (is @executed?*)
    (is (= expected-result result))))

(deftest ^:async canonical-start-failure-prevents-false-execution-test
  (let [executed?* (atom false)
        config {:clio/correlation-id-fn
                (sequential-id-fn ["turn-1" "episode-1" "event-1"])
                :clio/append!
                (fn [_envelope]
                  (js/Promise.reject (js/Error. "ledger unavailable")))
                :turn-executor!
                (fn [_runtime _config _request]
                  (reset! executed?* true)
                  (js/Promise.resolve {:answer "should not run"}))}]
    (try
      (await (episode-turn/send-agent-turn! nil config turn-request))
      (is false "canonical start append should reject")
      (catch :default error
        (is (= "ledger unavailable" (.-message error)))))
    (is (false? @executed?*))))

(deftest ^:async rejected-turn-start-closes-accepted-run-test
  (let [accepted-events* (atom [])
        append-count* (atom 0)
        executed?* (atom false)
        turn-start-error (js/Error. "turn start rejected")
        config {:clio/correlation-id-fn
                (sequential-id-fn
                 ["turn-1" "episode-1"
                  "event-1" "event-2" "event-3" "event-4"])
                :clio/append!
                (fn [envelope]
                  (let [append-number (swap! append-count* inc)]
                    (if (= 2 append-number)
                      (js/Promise.reject turn-start-error)
                      (do
                        (swap! accepted-events* conj envelope)
                        (js/Promise.resolve :appended)))))
                :turn-executor!
                (fn [_runtime _config _request]
                  (reset! executed?* true)
                  (js/Promise.resolve {:answer "should not run"}))}]
    (try
      (await (episode-turn/send-agent-turn! nil config turn-request))
      (is false "turn-start append should reject")
      (catch :default actual-error
        (is (identical? turn-start-error actual-error))))
    (is (false? @executed?*))
    ;; The compensating emit resolves the pending turn start first (it is
    ;; admitted on retry), so the admitted turn is closed before the run.
    (is (= [:sol.run/started :sol.turn/started :sol.turn/failed :sol.run/failed]
           (mapv :event/type @accepted-events*)))
    (is (= "turn start rejected"
           (get-in (nth @accepted-events* 2) [:event/data :payload :error]))
        "the admitted turn start is closed with the original error")
    (is (= (into [nil] (butlast (mapv :event/id @accepted-events*)))
           (mapv #(first (:event/causes %)) @accepted-events*)))
    (is (= "turn start rejected"
           (get-in (last @accepted-events*) [:event/data :payload :error])))))

(deftest ^:async original-and-ledger-failures-are-both-observable-test
  (let [append-count* (atom 0)
        config {:clio/correlation-id-fn
                (sequential-id-fn
                 ["turn-1" "episode-1"
                  "event-1" "event-2" "event-3"])
                :clio/append!
                (fn [_envelope]
                  (swap! append-count* inc)
                  (if (= 3 @append-count*)
                    (js/Promise.reject (js/Error. "failure event rejected"))
                    (js/Promise.resolve :appended)))
                :turn-executor!
                (fn [_runtime _config _request]
                  (js/Promise.reject (js/Error. "provider failed")))}]
    (try
      (await (episode-turn/send-agent-turn! nil config turn-request))
      (is false "combined failure should reject")
      (catch :default error
        (is (= "Sol turn and canonical episode emission failed"
               (.-message error)))
        (is (= "provider failed"
               (:turn-error (ex-data error))))
        (is (= "failure event rejected"
               (:ledger-error (ex-data error))))))))
