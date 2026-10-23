(ns open-hax.sol.shape.episode-event-test
  (:require [cljs.test :refer [deftest is testing]]
            [clio.domain.schema :as schema]
            [clio.infra.event :as event]
            [open-hax.sol.infra.agent.clio-store :as store]
            [open-hax.sol.shape.episode-event :as episode-event]))

(def agent-spec
  {:actor-id "actor.agent.research"
   :contract-id "agent/research"
   :contract-revision "git:abc123"})

(def runtime-binding
  {:binding/version 1
   :principal/actor-id "actor.agent.research"
   :principal/entity-id "entity.agent.research"
   :principal/kind "agent"
   :principal/org-id "org.open-hax"})

(def expected-ledger-binding
  (assoc runtime-binding
         :actor/resource {:resource/id "agent/research"
                          :resource/revision "git:abc123"}))

(deftest principal-binding-consumes-axxium-authority-test
  (testing "canonical Axxium binding is preserved and augmented with resource lineage"
    (is (= expected-ledger-binding
           (episode-event/principal-binding runtime-binding agent-spec))))

  (testing "nested canonical binding with namespaced JSON string keys is accepted"
    (is (= expected-ledger-binding
           (episode-event/principal-binding
            {"principal/binding"
             {"binding/version" 1
              "principal/actor-id" "actor.agent.research"
              "principal/entity-id" "entity.agent.research"
              "principal/kind" "agent"
              "principal/org-id" "org.open-hax"}}
            agent-spec))))

  (testing "Axxium auth identity without authoritative kind does not fabricate a binding"
    (is (nil? (episode-event/principal-binding
               {:auth/actor-id "actor.agent.research"
                :auth/entity-id "entity.agent.research"
                :auth/org-id "org.open-hax"}
               agent-spec))))

  (testing "legacy complete aliases remain compatible"
    (is (= expected-ledger-binding
           (episode-event/principal-binding
            {:actorId "actor.agent.research"
             :entityId "entity.agent.research"
             :orgId "org.open-hax"
             :principalKind "agent"}
            agent-spec))))

  (testing "missing entity identity never fabricates a binding"
    (is (nil? (episode-event/principal-binding
               {:actorId "actor.agent.research"
                :orgId "org.open-hax"
                :principalKind "agent"}
               agent-spec))))

  (testing "unknown principal kind never falls back into an identity binding"
    (is (nil? (episode-event/principal-binding
               {:actorId "actor.service"
                :entityId "entity.service"
                :principalKind "robot"}
               {}))))

  (testing "unsupported binding versions fail explicitly"
    (try
      (episode-event/principal-binding
       (assoc runtime-binding :binding/version 2)
       agent-spec)
      (is false "unsupported binding version should throw")
      (catch :default error
        (is (= 2 (:binding/version (ex-data error))))))))

(deftest actor-attribution-without-binding-test
  (let [actor (episode-event/actor-descriptor
               {:auth/actor-id "actor.agent.research"
                :auth/entity-id "entity.agent.research"}
               agent-spec
               "sol.node.local")]
    (is (= "actor.agent.research" (:actor-id actor)))
    (is (= "agent" (:actor-kind actor)))
    (is (= "sol.node.local" (:actor-node actor)))
    (is (not (contains? actor :principal/binding)))))

(deftest principal-kind-vocabulary-test
  (doseq [kind ["human" "agent" "service" "automation"]]
    (let [binding (episode-event/principal-binding
                   {:binding/version 1
                    :principal/actor-id (str "actor." kind)
                    :principal/entity-id (str "entity." kind)
                    :principal/kind kind}
                   {})]
      (is (= kind (:principal/kind binding))))))

(deftest episode-clio-correspondence-test
  (let [root "00000000-0000-4000-8000-000000000001"
        context (episode-event/episode-context
                 {:run-id "run-1" :session-id "session-1" :turn-id "turn-1"
                  :episode-id "episode-1" :conversation-id "conversation-1"
                  :causal-root root :node-id "sol.node.local"
                  :auth-context {:principal/binding runtime-binding}
                  :agent-spec agent-spec})
        revision (store/current-revision nil)
        facts (episode-event/event-facts context 2 root {:status "running"})
        constructed (event/make-event revision :sol.turn/started facts)
        data (:event/data constructed)]
    (is (= "run-1" (:run/id data)))
    (is (= "session-1" (:session/id data)))
    (is (= "turn-1" (:turn/id data)))
    (is (= "episode-1" (:episode/id data)))
    (is (= root (:causal/root data)))
    (is (= [root] (:event/causes constructed)))
    (is (= 2 (:event/seq constructed)))
    (is (= "episode-1" (:event/stream constructed)))
    (is (= "conversation-1" (get-in data [:payload :conversation/id])))
    (is (= ["agent/research"] (:contracts data)))
    (is (= [{:resource/id "agent/research" :resource/revision "git:abc123"}]
           (:contract/refs data)))
    (is (= expected-ledger-binding (get-in data [:event/from :principal/binding])))
    (is (= constructed (schema/validate-event! [revision] constructed)))))
