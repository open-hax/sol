(ns open-hax.sol.infra.core
  "Minimal Sol core: exposes route and WebSocket registration helpers."
  (:require [open-hax.sol.infra.routes.app :as app-routes]
            [open-hax.sol.infra.agent.clio-store :as clio-store]
            [open-hax.sol.infra.agent.run-state :as run-state]
            [open-hax.sol.infra.agent.session-store :as session-store]
            [open-hax.sol.domain.node.path :as path]
            [open-hax.sol.domain.realtime :as realtime]
            [open-hax.sol.infra.config :as runtime-config]
            [open-hax.sol.domain.models :as runtime-models]
            [open-hax.sol.extern.process :as process]
            [open-hax.sol.runtime.state :as runtime-state]))

(defonce server* (atom nil))

(defn register-ws-routes!
  [runtime app]
  (realtime/register-ws-routes! runtime app (fn [] 0) (atom [])))

(defn config-js
  []
  (clj->js (runtime-models/enrich-config (runtime-config/cfg) process/env-var)))

(defn resolve-app-config
  "Every exported startup seam resolves the same config, including the opt-in
   Clio store, so SOL_CLIO_* paths never reach routes without :clio/store."
  [config]
  (clio-store/ensure-configured!
   (runtime-models/enrich-config
    (if (map? config) config (runtime-config/cfg))
    process/env-var)))

(defonce ^{:doc "The session store this path installed, and whether a route
   registration has run with it: {:store s :active? bool}, or nil."}
  auto-session-store* (atom nil))

(defn ensure-default-stores!
  "start! and solPlugin skip bootstrap!, so install the same default session
   and run stores here when none is configured yet. bootstrap!'s stores, and
   any store installed explicitly, win. A :clio/store that names the same
   ledger as the auto-created session store's store is replaced by that store
   in the returned config, so routes and session events share one store
   object. Once any registration with a session store this path created has
   run, even one that threw partway, a different ledger is rejected before
   any shared state changes.
   Returns the config to register with."
  [resolved-config]
  (let [base-dir (path/join (path/cwd) ".ημ" "sol")
        canonical (:clio/store resolved-config)
        current @session-store/default-store*
        {auto :store active? :active?} @auto-session-store*
        ours? (and (some? current) (identical? current auto))
        bound (when ours? (:canonical-store current))
        same? (and ours? (clio-store/same-store? canonical bound))
        stale? (and ours? (not same?))]
    (when (and stale? active?)
      (throw (ex-info "A Sol app is already registered with a different :clio/store; the process-wide session store cannot be rebound while it runs."
                      {:type :sol/incompatible-clio-store})))
    (when (or (nil? current) stale?)
      (let [store (session-store/create-edn-session-store
                   (path/join base-dir "sessions") canonical)]
        (reset! auto-session-store* {:store store :active? false})
        (session-store/set-default-store! store)))
    (when-not @run-state/default-store*
      (run-state/set-default-store!
       (run-state/create-edn-run-state-store (path/join base-dir "runs"))))
    (if (and same? (some? bound))
      (assoc resolved-config :clio/store bound)
      resolved-config)))

(defn- mark-auto-store-active! []
  (swap! auto-session-store*
         (fn [{:keys [store] :as state}]
           (if (and (some? store) (identical? store @session-store/default-store*))
             (assoc state :active? true)
             state))))

(defn register-app-routes!
  [runtime app config]
  (let [resolved-config (ensure-default-stores! (resolve-app-config config))]
    (reset! runtime-state/config* resolved-config)
    (reset! runtime-state/runtime* runtime)
    ;; register-routes! attaches routes one at a time, so a throw can leave
    ;; some Sol handlers on the caller's app. They already use the session
    ;; store, so pin it on failure too: a later registration naming a
    ;; different ledger is then rejected instead of rebinding it.
    (try
      (app-routes/register-routes! runtime app resolved-config)
      (finally
        (mark-auto-store-active!)))))

(defn ^:async start!
  [runtime]
  (when-not @server*
    (let [config (resolve-app-config nil)
          Fastify (js/require "fastify")
          app (Fastify #js {:logger #js {:stream (.-stderr js/process)}})]
      ;; register-app-routes! writes config*/runtime* only after its store
      ;; compatibility check, so a rejected start! leaves them unchanged.
      (register-app-routes! runtime app config)
      (register-ws-routes! runtime app)
      (await (.listen app #js {:host (:host config) :port (:port config)}))
      (reset! server* app)
      (.info (.-log app) (str "Sol backend CLJS listening on " (:host config) ":" (:port config))))))
