#!/usr/bin/env bb
;; Structural check of a proposal only; not a runtime, resource or board validator.
(require '[clojure.edn :as edn]
         '[clojure.java.io :as io]
         '[clojure.string :as str]
         '[rewrite-clj.node :as node]
         '[rewrite-clj.parser :as parser])

(defn require-check! [valid? message]
  (when-not valid?
    (throw (ex-info message {}))))

(def expected-integration-anchors
  "Current bounded source inventory and the integration point each path must
   name, independent of the proposal being checked."
  {"src/cljs/open_hax/sol/infra/agent/service.cljs"                  "IAgentService"
   "src/cljs/open_hax/sol/infra/agent/runner.cljs"                   "spawn-direct!"
   "src/cljs/open_hax/sol/infra/agent/provider/turn_processor.cljs"  "IAgentProviderAdapter"
   "src/cljs/open_hax/sol/infra/agent/turn_session.cljs"             "abort!"
   "src/cljs/open_hax/sol/infra/agent/run_state.cljs"                "IRunStateStore"
   "src/cljs/open_hax/sol/infra/agent/session_store.cljs"            "ISessionStore"
   "src/cljs/open_hax/sol/infra/agent/clio_store.cljs"               "open!"
   "src/cljs/open_hax/sol/law/lifecycle_catalog.cljc"                "schema/event-schema"
   "src/cljs/open_hax/sol/infra/agent/episode_ledger.cljs"           "configured-appender"
   "src/cljs/open_hax/sol/infra/agent/episode_turn.cljs"             "send-agent-turn!"
   "src/cljs/open_hax/sol/shape/episode_event.cljs"                  "principal-binding"
   "src/cljs/open_hax/sol/law/contract_kinds.cljs"                   "class->kind"
   "src/cljs/open_hax/sol/domain/contracts/loader.cljs"              "parse-contract-file-records!"
   "src/cljs/open_hax/sol/bootstrap.cljs"                            "bootstrap!"
   "src/cljs/open_hax/sol/infra/config.cljs"                         "cfg"
   "src/cljs/open_hax/sol/infra/graceful_shutdown.cljs"              "begin-shutdown!"
   "src/cljs/open_hax/sol/infra/routes/app.cljs"                     "api-agent-session-abort!"
   "src/cljs/open_hax/sol/infra/agent/runtime.cljs"                  "queue-agent-control!"})

(def expected-integration-paths (set (keys expected-integration-anchors)))

(defn declared-git-pins [deps]
  (into {}
        (keep (fn [[lib options]]
                (when (:git/sha options)
                  [lib (select-keys options [:git/sha :git/tag :deps/root])])))
        (:deps deps)))

(defn code-symbols
  "Symbol tokens the reader keeps in source. Comments, `#_` discarded forms,
   strings, regexes, character literals and keywords are excluded, so anchors
   match only active code."
  [source]
  (let [root (parser/parse-string-all source)
        kept? (fn [node] (and (node/inner? node) (not= :uneval (node/tag node))))]
    (into #{}
          (comp (filter #(and (= :token (node/tag %)) (symbol? (:value %))))
                (map node/string))
          (tree-seq kept? node/children root))))

(defn code-token?
  "True when anchor is a whole symbol token in active code, not part of a
   longer symbol, a comment, a string or a discarded form."
  [source anchor]
  (contains? (code-symbols source) anchor))

(defn check-plan!
  ([proposal-path] (check-plan! proposal-path "."))
  ([proposal-path source-root]
  (let [plan (edn/read-string (slurp proposal-path))
        deps (edn/read-string (slurp "deps.edn"))
        expected-pins (declared-git-pins deps)
        points (:integration/points plan)]
    (require-check! (= :proposed (:proposal/status plan)) "Proposal must remain proposed")
    (require-check! (false? (:activation/enabled? plan)) "Proposal must not activate a service")
    (require-check! (re-matches #"[0-9a-f]{40}" (:source/revision plan)) "Full source SHA required")
    (require-check! (= "330aa63f-f697-5bd6-9fc0-3f19fbea2be4" (:parent/story plan))
                    "Expected Foresight parent story")
    (require-check! (= :eta-mu (get-in plan [:authority :publication])) "Publisher owner drift")
    (require-check! (false? (get-in plan [:review-policy :auto-merge?])) "Planning auto-merge must stay off")
    (require-check! (= (set (keys expected-pins)) (set (keys (:dependencies plan))))
                    "Incomplete or extra dependency inventory")
    (doseq [[lib pin] (:dependencies plan)]
      (require-check! (= pin (get expected-pins lib))
                      (str "Dependency pin drift: " lib)))
    (require-check! (= expected-integration-paths (set (map :path points)))
                    "Incomplete or extra integration inventory")
    (require-check! (= (count points) (count (set (map :path points)))) "Duplicate source paths")
    (doseq [{:keys [path namespace anchor gap]} points]
      (require-check! (and (keyword? gap) (not (str/blank? (name gap))))
                      (str "Missing integration gap: " path))
      (require-check! (.isFile (io/file source-root path)) (str "Missing integration file: " path))
      (let [source (slurp (io/file source-root path))
            ns-form (binding [*read-eval* false]
                      (read (java.io.PushbackReader. (java.io.StringReader. source))))]
        (require-check! (and (= 'ns (first ns-form))
                             (= (symbol namespace) (second ns-form)))
                        (str "Namespace drift: " path))
        (require-check! (and (string? anchor) (not (str/blank? anchor)))
                        (str "Blank integration anchor: " path))
        (require-check! (= (get expected-integration-anchors path) anchor)
                        (str "Unexpected integration anchor: " path))
        (require-check! (code-token? source anchor) (str "Anchor drift: " path))))
    {:proposal (:proposal/id plan) :status :proposed
     :dependency-pins (count (:dependencies plan)) :integration-files (count points)
     :runtime-validated? false :board-validated? false :deployment-validated? false})))

(try
  (prn (check-plan! (or (first *command-line-args*) "docs/design/persistent-review-workers.edn")
                    (or (second *command-line-args*) ".")))
  (catch Exception error
    (binding [*out* *err*] (println "Proposal check failed:" (.getMessage error)))
    (System/exit 1)))
