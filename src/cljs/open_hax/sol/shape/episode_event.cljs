(ns open-hax.sol.shape.episode-event
  "Sol lifecycle facts carried by Clio. Supplied Axxium principal and Katamorph
   resource references are correlation data, not locally invented authority."
  (:require [clojure.string :as str]))

(def principal-kinds
  #{"human" "agent" "service" "automation"})

(defn- nonblank-string
  [value]
  (some-> value str str/trim not-empty))

(defn- key-string
  [key]
  (if (keyword? key)
    (if-let [key-ns (namespace key)]
      (str key-ns "/" (name key))
      (name key))
    (str key)))

(defn- first-value
  [m keys]
  (some (fn [key]
          (let [string-key (key-string key)]
            (cond
              (contains? m key) (get m key)
              (contains? m string-key) (get m string-key)
              :else nil)))
        keys))

(defn- shaped-string
  [m keys]
  (nonblank-string (first-value (or m {}) keys)))

(defn- binding-source
  [auth-context]
  (let [auth-context (or auth-context {})
        nested (first-value auth-context
                            [:principal/binding
                             :principalBinding
                             :principal-binding
                             :principal_binding
                             :runtimeBinding
                             :runtime-binding
                             :runtime_binding])]
    (if (map? nested) nested auth-context)))

(defn- supplied-principal-kind
  [auth-context]
  (let [source (binding-source auth-context)
        requested (some-> (or (shaped-string source
                                              [:principal/kind
                                               :principalKind
                                               :principal-kind
                                               :principal_kind])
                              (shaped-string auth-context
                                             [:auth/principal-kind
                                              :auth/principalKind
                                              :principalKind
                                              :principal-kind
                                              :principal_kind]))
                          str/lower-case)]
    (when (contains? principal-kinds requested)
      requested)))

(defn- binding-version
  [source]
  (let [value (first-value source
                           [:binding/version
                            :bindingVersion
                            :binding-version
                            :binding_version])]
    (cond
      (nil? value) 1
      (= 1 value) 1
      (= "1" (str value)) 1
      :else
      (throw (ex-info "Unsupported Axxium runtime binding version"
                      {:binding/version value})))))

(defn resource-ref
  "Project the governing actor/agent resource reference from an agent spec."
  [agent-spec]
  (when-let [resource-id (nonblank-string (:contract-id agent-spec))]
    (cond-> {:resource/id resource-id}
      (nonblank-string (:contract-revision agent-spec))
      (assoc :resource/revision
             (nonblank-string (:contract-revision agent-spec))))))

(defn principal-binding
  "Preserve the supplied Axxium identity binding and resource correlation.

   Accepted input includes Axxium's canonical RuntimePrincipalBinding map,
   a nested `:principal/binding`, Axxium `:auth/*` identity keys accompanied by
   a supplied principal kind, and legacy transport aliases. Agent specs may add
   a Katamorph resource reference but never fabricate principal identity/kind."
  [auth-context agent-spec]
  (let [source (binding-source auth-context)
        actor-id (or (shaped-string source
                                    [:principal/actor-id
                                     :actorId :actor-id :actor_id])
                     (shaped-string auth-context
                                    [:auth/actor-id
                                     :actorId :actor-id :actor_id]))
        entity-id (or (shaped-string source
                                     [:principal/entity-id
                                      :entityId :entity-id :entity_id
                                      :principalEntityId :principal-entity-id
                                      :principal_entity_id])
                      (shaped-string auth-context
                                     [:auth/entity-id
                                      :entityId :entity-id :entity_id]))
        org-id (or (shaped-string source
                                  [:principal/org-id
                                   :orgId :org-id :org_id
                                   :tenantId :tenant-id :tenant_id])
                   (shaped-string auth-context
                                  [:auth/org-id
                                   :orgId :org-id :org_id
                                   :tenantId :tenant-id :tenant_id]))
        principal-kind (supplied-principal-kind auth-context)
        actor-resource (resource-ref agent-spec)]
    (when (and actor-id entity-id principal-kind)
      (cond-> {:binding/version (binding-version source)
               :principal/actor-id actor-id
               :principal/entity-id entity-id
               :principal/kind principal-kind}
        org-id (assoc :principal/org-id org-id)
        actor-resource (assoc :actor/resource actor-resource)))))

(defn- transport-actor-kind
  [auth-context agent-spec]
  (or (supplied-principal-kind auth-context)
      (when (nonblank-string (:actor-id agent-spec)) "agent")
      "service"))

(defn actor-descriptor
  "Build transport attribution. The actor descriptor may exist without a
   principal binding; its fallback identifies the Sol runtime node, not an
   invented authenticated principal."
  [auth-context agent-spec node-id]
  (let [binding (principal-binding auth-context agent-spec)
        source (binding-source auth-context)
        actor-id (or (:principal/actor-id binding)
                     (shaped-string source
                                    [:principal/actor-id
                                     :actorId :actor-id :actor_id])
                     (shaped-string auth-context
                                    [:auth/actor-id
                                     :actorId :actor-id :actor_id])
                     (nonblank-string (:actor-id agent-spec))
                     (nonblank-string node-id)
                     "sol.runtime")
        actor-kind (or (:principal/kind binding)
                       (transport-actor-kind auth-context agent-spec))]
    (cond-> {:actor-id actor-id
             :actor-kind actor-kind}
      (nonblank-string node-id) (assoc :actor-node (nonblank-string node-id))
      binding (assoc :principal/binding binding))))

(defn episode-context
  "Construct the immutable identity/correlation portion of one Sol turn
   episode. ID generation belongs to infra and is supplied by the caller."
  [{:keys [run-id session-id turn-id episode-id conversation-id causal-root
           node-id auth-context agent-spec]}]
  (let [actor (actor-descriptor auth-context agent-spec node-id)
        contract-ref (resource-ref agent-spec)]
    (cond-> {:run/id run-id
             :session/id session-id
             :turn/id turn-id
             :episode/id episode-id
             :causal/root causal-root
             :event/from actor}
      (nonblank-string conversation-id)
      (assoc :conversation/id (nonblank-string conversation-id))

      contract-ref
      (assoc :contracts [(:resource/id contract-ref)]
             :contract/refs [contract-ref]))))

(defn event-facts
  "Project episode correlation and payload into Clio domain/stream facts.
   Clio constructs and validates the generic envelope."
  [episode sequence parent-id payload]
  {:event/stream (:episode/id episode)
   :event/seq sequence
   :event/causes (if parent-id [parent-id] [])
   :event/actor (get-in episode [:event/from :actor-id])
   :event/subject (:run/id episode)
   :event/data
   (cond-> (assoc (dissoc episode :causal/root)
                  :delivery/mode "stream"
                  :payload (cond-> (or payload {})
                             (:conversation/id episode)
                             (assoc :conversation/id (:conversation/id episode))))
     (:causal/root episode) (assoc :causal/root (:causal/root episode))
     parent-id (assoc :causal/parent parent-id))})
