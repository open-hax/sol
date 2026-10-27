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

(defn ensure-default-stores!
  "start! and solPlugin skip bootstrap!, so install the same default session
   and run stores here when none is configured yet. bootstrap!'s stores win."
  [resolved-config]
  (let [base-dir (path/join (path/cwd) ".ημ" "sol")]
    (when-not @session-store/default-store*
      (session-store/set-default-store!
       (session-store/create-edn-session-store
        (path/join base-dir "sessions") (:clio/store resolved-config))))
    (when-not @run-state/default-store*
      (run-state/set-default-store!
       (run-state/create-edn-run-state-store (path/join base-dir "runs"))))))

(defn register-app-routes!
  [runtime app config]
  (let [resolved-config (resolve-app-config config)]
    (ensure-default-stores! resolved-config)
    (reset! runtime-state/config* resolved-config)
    (reset! runtime-state/runtime* runtime)
    (app-routes/register-routes! runtime app resolved-config)))

(defn ^:async start!
  [runtime]
  (when-not @server*
    (let [config (resolve-app-config nil)
          Fastify (js/require "fastify")
          app (Fastify #js {:logger #js {:stream (.-stderr js/process)}})]
      (reset! runtime-state/config* config)
      (reset! runtime-state/runtime* runtime)
      (register-app-routes! runtime app config)
      (register-ws-routes! runtime app)
      (await (.listen app #js {:host (:host config) :port (:port config)}))
      (reset! server* app)
      (.info (.-log app) (str "Sol backend CLJS listening on " (:host config) ":" (:port config))))))
