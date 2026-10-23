(ns open-hax.sol.infra.agent.episode-ledger-test
  (:require [cljs.test :refer [deftest is]]
            [clio.domain.schema :as schema]
            [open-hax.sol.infra.agent.episode-ledger :as episode-ledger]))

(def episode-input
  {:run-id "run-1" :session-id "session-1" :conversation-id "conversation-1"
   :agent-spec {:actor-id "actor.agent.research" :contract-id "agent/research"
                :contract-revision "git:abc123"}
   :auth-context {:actorId "actor.agent.research" :entityId "entity.agent.research"
                  :orgId "org.open-hax" :principalKind "agent"}})

(deftest ^:async causal-lifecycle-test
  (let [accepted* (atom [])
        episode (episode-ledger/create-episode
                 {:sol-node-id "sol.node.local"
                  :clio/append! (fn [event]
                                  (swap! accepted* conj event)
                                  (js/Promise.resolve :appended))}
                 episode-input)]
    (doseq [type [:sol.run/started :sol.turn/started
                  :sol.turn/completed :sol.run/completed]]
      (await (episode-ledger/emit! episode type {:status "ok"})))
    (let [events @accepted* ids (mapv :event/id events)]
      (is (= [:sol.run/started :sol.turn/started :sol.turn/completed :sol.run/completed]
             (mapv :event/type events)))
      (is (= [1 2 3 4] (mapv :event/seq events)))
      (is (= [[] [(ids 0)] [(ids 1)] [(ids 2)]] (mapv :event/causes events)))
      (is (= #{(first ids)} (set (map #(get-in % [:event/data :causal/root]) events))))
      (is (= 1 (count (set (map :event/stream events)))))
      (is (= #{"run-1"} (set (map #(get-in % [:event/data :run/id]) events))))
      (is (= #{"org.open-hax"}
             (set (map #(get-in % [:event/data :event/from :principal/binding
                                   :principal/org-id]) events))))
      (doseq [event events]
        (is (= event (schema/validate-event! [(:revision episode)] event)))))))

(deftest ^:async no-appender-remains-valid-test
  (let [episode (episode-ledger/create-episode {} (assoc episode-input :auth-context nil))
        event (await (episode-ledger/emit! episode "sol.run.started" {:status "running"}))]
    (is (false? (episode-ledger/configured? episode)))
    (is (= :sol.run/started (:event/type event)))
    (is (= (:event/id event) (get-in event [:event/data :causal/root])))
    (is (= "actor.agent.research" (:event/actor event)))
    (is (not (contains? (get-in event [:event/data :event/from]) :principal/binding)))
    (is (= event (schema/validate-event! [(:revision episode)] event)))))

(deftest unsupported-mongo-test
  (is (thrown-with-msg? cljs.core/ExceptionInfo #"Mongo"
        (episode-ledger/configured-appender {:event-ledger-db {:name "old-db"}})))
  (is (thrown-with-msg? cljs.core/ExceptionInfo #"explicit"
        (episode-ledger/configured-appender {:event-ledger-append! identity}))))

(deftest ^:async retry-retains-constructed-event-test
  (let [attempts* (atom [])
        episode (episode-ledger/create-episode
                 {:clio/append! (fn [event]
                                  (swap! attempts* conj event)
                                  (if (= 1 (count @attempts*))
                                    (js/Promise.reject (js/Error. "ack lost after append"))
                                    (js/Promise.resolve :already-present)))}
                 episode-input)]
    (try
      (await (episode-ledger/emit! episode "sol.run.started" {:status "running"}))
      (is false "first acknowledgement should reject")
      (catch :default _ nil))
    (is (= 0 @(:sequence* episode)))
    (is (nil? @(:parent-id* episode)))
    (let [accepted (await (episode-ledger/emit! episode "sol.run.started" {:status "running"}))]
      (is (= (first @attempts*) (second @attempts*) accepted))
      (is (= 1 (:event/seq accepted)))
      (is (= [] (:event/causes accepted)))
      (is (= 1 @(:sequence* episode))))))

(deftest ^:async non-admission-result-is-not-accepted-test
  (let [episode (episode-ledger/create-episode {:clio/append! identity} episode-input)]
    (try
      (await (episode-ledger/emit! episode :sol.run/started {}))
      (is false "Returning event data is not upstream append admission")
      (catch :default error
        (is (re-find #"admission result" (.-message error)))))
    (is (= 0 @(:sequence* episode)))
    (is (some? @(:pending* episode)))))

(deftest ^:async equivalent-empty-payload-retries-one-event-test
  (let [attempts* (atom [])
        e (episode-ledger/create-episode
           {:clio/append! (fn [event]
                            (swap! attempts* conj event)
                            (if (= 1 (count @attempts*))
                              (throw (ex-info "lost acknowledgement" {}))
                              :appended))}
           episode-input)]
    (try (await (episode-ledger/emit! e :sol.run/started nil)) (catch :default _ nil))
    (await (episode-ledger/emit! e :sol.run/started {}))
    (is (= 2 (count @attempts*)) "Equivalent shaped payload must retry only")
    (is (= 1 @(:sequence* e)))))
