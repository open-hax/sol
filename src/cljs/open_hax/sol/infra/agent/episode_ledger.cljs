(ns open-hax.sol.infra.agent.episode-ledger
  "Clio lifecycle coordination for one existing Sol turn. Pending identity is
   process-local; this is not a restart intent store, lease or durable-ack layer."
  (:require [clio.domain.schema :as schema]
            [clio.infra.event :as event]
            [open-hax.sol.infra.agent.clio-store :as store]
            [open-hax.sol.law.lifecycle-catalog :as lifecycle]
            [open-hax.sol.shape.episode-event :as episode-event]))

(defn configured-appender
  "Resolve an explicit Clio appender; only upstream admission results qualify."
  [config]
  (store/require-supported-config! config)
  (when (and (or (:clio/ledger-file config) (:clio/schema-directory config))
             (not (:clio/store config)))
    (throw (ex-info "Configure the Clio filesystem store before creating episodes" {})))
  (or (:clio/append! config)
      (when-let [clio-store (:clio/store config)]
        (fn [constructed-event] (store/append! clio-store constructed-event)))))

(defn create-episode
  "Create process-local stream coordination. One stream per episode."
  [config {:keys [run-id session-id conversation-id agent-spec auth-context]}]
  (let [id-fn (or (:clio/correlation-id-fn config) #(str (random-uuid)))
        turn-id (id-fn)
        episode-id (id-fn)
        base (episode-event/episode-context
              {:run-id run-id :session-id session-id :turn-id turn-id
               :episode-id episode-id :conversation-id conversation-id
               :node-id (:sol-node-id config) :auth-context auth-context
               :agent-spec agent-spec})]
    {:context base
     :revision (store/current-revision (:clio/store config))
     :append! (configured-appender config)
     :sequence* (atom 0)
     :root-id* (atom nil)
     :parent-id* (atom nil)
     :pending* (atom nil)
     :emitting?* (atom false)}))

(defn configured?
  [episode]
  (some? (:append! episode)))

(defn- construct-event
  [episode schema-id payload]
  (let [revision (:revision episode)
        facts (episode-event/event-facts
               (assoc (:context episode) :causal/root @(:root-id* episode))
               (inc @(:sequence* episode)) @(:parent-id* episode) payload)
        constructed (event/make-event revision schema-id facts)
        correlated (assoc-in constructed [:event/data :causal/root]
                             (or @(:root-id* episode) (:event/id constructed)))]
    ;; Root correlation is Sol data. Generic identity/envelope validation is Clio.
    (schema/validate-event! [revision] correlated)))

(defn- ^:async accept-pending!
  [episode]
  (let [constructed (:event @(:pending* episode))]
    (when-let [append! (:append! episode)]
      (let [verdict (await (append! constructed))]
        (when-not (contains? #{:appended :already-present} verdict)
          (throw (ex-info "Clio appender did not return an admission result"
                          {:append/result verdict})))))
    (reset! (:sequence* episode) (:event/seq constructed))
    (compare-and-set! (:root-id* episode) nil (:event/id constructed))
    (reset! (:parent-id* episode) (:event/id constructed))
    (reset! (:pending* episode) nil)
    constructed))

(defn ^:async emit!
  "Construct once, retain exact data on ambiguous failure and retry upstream
   admission. Resolve any pending write before emitting a different lifecycle
   fact, including compensation. Validation-only mode is not persistence."
  [episode event-type payload]
  (when-not (compare-and-set! (:emitting?* episode) false true)
    (throw (ex-info "Concurrent emission on one Sol episode is unsupported" {})))
  (try
    (let [schema-id (lifecycle/schema-id event-type)
          intent {:type schema-id :payload (or payload {})}]
      (when-let [pending @(:pending* episode)]
        (when-not (= intent (select-keys pending [:type :payload]))
          (await (accept-pending! episode))))
      (when-not @(:pending* episode)
        (reset! (:pending* episode)
                (assoc intent :event (construct-event episode schema-id payload))))
      (await (accept-pending! episode)))
    (finally
      (reset! (:emitting?* episode) false))))
