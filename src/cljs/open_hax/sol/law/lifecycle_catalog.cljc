(ns open-hax.sol.law.lifecycle-catalog
  "Sol-owned lifecycle data. The complete envelope/schema law belongs to Clio."
  (:require [clio.law.schema :as schema]))

(def lifecycle-types
  {"sol.run.started" :sol.run/started
   "sol.run.failed" :sol.run/failed
   "sol.run.completed" :sol.run/completed
   "sol.turn.started" :sol.turn/started
   "sol.turn.failed" :sol.turn/failed
   "sol.turn.completed" :sol.turn/completed})

(def lifecycle-data
  [:map {:closed true}
   [:run/id [:string {:min 1}]]
   [:session/id [:string {:min 1}]]
   [:turn/id [:string {:min 1}]]
   [:episode/id [:string {:min 1}]]
   ;; Already shaped attribution/binding/resource facts; their owners retain
   ;; authority. This catalog does not restate Axxium or Katamorph contracts.
   [:event/from :map]
   [:conversation/id {:optional true} :string]
   [:causal/root {:optional true} :string]
   [:causal/parent {:optional true} :string]
   [:contracts {:optional true} [:vector :string]]
   [:contract/refs {:optional true} [:vector :map]]
   [:delivery/mode [:= "stream"]]
   [:payload :map]])

(def catalog
  (into {} (map (fn [id] [id (schema/event-schema id lifecycle-data)]))
        (vals lifecycle-types)))

(defn schema-id
  "Resolve only the six supported Sol lifecycle types."
  [event-type]
  (or (get lifecycle-types event-type)
      (when (contains? catalog event-type) event-type)
      (throw (ex-info "Unsupported Sol lifecycle type" {:event/type event-type}))))
