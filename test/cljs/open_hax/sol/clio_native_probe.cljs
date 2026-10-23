(ns open-hax.sol.clio-native-probe
  "Test-only compiled ESM probe of Sol's actual Clio transport, never inference."
  (:require [clio.extern.js.fs :as fs]
            [open-hax.sol.infra.agent.clio-store :as store]
            [open-hax.sol.infra.agent.episode-ledger :as episode]
            [open-hax.sol.infra.agent.session-store :as sessions]
            [open-hax.sol.domain.node.path :as path]))

(defn- require-result!
  [predicate message]
  (when-not predicate (throw (ex-info message {}))))

(defn- ^:async require-session-rejection!
  [invoke expected-data]
  (let [pending (invoke)]
    (require-result! (instance? js/Promise pending) "Session call did not return a Promise")
    (let [error (try (await pending) nil (catch :default error error))]
      (require-result! (instance? cljs.core/ExceptionInfo error) "Session call did not reject with original error")
      (doseq [[key value] expected-data]
        (require-result! (= value (get (ex-data error) key)) "Session rejection lost original error facts")))))

(defn ^:async probe!
  [root]
  (let [options {:ledger-file (path/join root "epoch" "events.edn")
                 :schema-directory (path/join root "epoch" "schemas")}
        canonical-store (store/open! (assoc options :initialize? true))
        attempts* (atom [])
        e (episode/create-episode
           {:clio/store canonical-store
            :clio/append! (fn [event]
                            (let [verdict (store/append! canonical-store event)]
                              (swap! attempts* conj event)
                              (if (= 1 (count @attempts*))
                                (throw (ex-info "probe lost acknowledgement" {}))
                                verdict)))}
           {:run-id "probe-run" :session-id "probe-session"})]
    (try
      (await (episode/emit! e :sol.run/started {:status "running"}))
      (throw (ex-info "Lost acknowledgement fixture did not reject" {}))
      (catch :default error
        (require-result! (= "probe lost acknowledgement" (.-message error))
                         "Unexpected first append error")))
    (require-result! (= 0 @(:sequence* e)) "Sequence advanced without acknowledgement")
    (let [first-event (await (episode/emit! e :sol.run/started {:status "running"}))
          second-event (await (episode/emit! e :sol.run/completed {:status "completed"}))
          reopened (store/open! options)
          events (store/events reopened)
          session-store (sessions/create-edn-session-store (path/join root "views") reopened)
          pending (sessions/append-event! session-store "probe-session" first-event)]
      (require-result! (= (first @attempts*) (second @attempts*) first-event)
                       "Retry did not preserve exact event identity/data")
      (require-result! (= [first-event second-event] events) "Complete replay changed accepted data")
      (require-result! (= [1 2] (mapv :event/seq events)) "Stream revisions changed")
      (require-result! (= [[] [(:event/id first-event)]] (mapv :event/causes events))
                       "Explicit causal predecessor was lost")
      (require-result! (instance? js/Promise pending) "Session append success did not return a Promise")
      (require-result! (true? (await pending)) "Session append retry did not resolve true")
      (require-result! (= events (await (sessions/get-events session-store "probe-session")))
                       "Session read changed accepted history")
      (await (require-session-rejection!
              #(sessions/append-event! session-store "another-session" first-event)
              {:session-id "another-session"}))
      (await (require-session-rejection!
              #(sessions/append-event! session-store "probe-session"
                                      (assoc-in first-event [:event/data :payload :status] "changed"))
              {:clio/error :clio.ledger/id-collision}))
      (let [unconfigured (sessions/create-edn-session-store (path/join root "unconfigured"))]
        (await (require-session-rejection!
                #(sessions/append-event! unconfigured "probe-session" first-event) {}))
        (await (require-session-rejection! #(sessions/get-events unconfigured "probe-session") {})))
      (let [ledger-file (:ledger-file reopened)]
        (fs/append-text! ledger-file "{:broken\n")
        (let [malformed-bytes (fs/read-text ledger-file)]
          (await (require-session-rejection!
                  #(sessions/append-event! session-store "probe-session" first-event)
                  {:clio/error :clio.ledger/invalid-edn}))
          (await (require-session-rejection!
                  #(sessions/get-events session-store "probe-session")
                  {:clio/error :clio.ledger/invalid-edn}))
          (require-result! (= malformed-bytes (fs/read-text ledger-file))
                           "Rejected session call rewrote malformed ledger")))
      {:status :passed :events (count events) :identical-retry true
       :reopen-replay true :stream-sequences (mapv :event/seq events)
       :session-promises true :session-errors-preserved true})))

(defn ^:async probe-edn!
  "JS export returns printable evidence; all assertions run through real stores."
  [root]
  (pr-str (await (probe! root))))
