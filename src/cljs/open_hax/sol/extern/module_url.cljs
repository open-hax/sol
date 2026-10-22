(ns open-hax.sol.extern.module-url
  "Stable module-owned anchor for Sol's own install (ESM builds).
   Closure cannot compile import.meta, so each ESM module entry in
   shadow-cljs.edn :prepends one line recording its import.meta.url under a
   registry symbol; this namespace only reads it. The CommonJS test build
   aliases this namespace to module-url-cjs.")

(defn module-url
  "URL (file://...) of the loaded Sol module file, or nil."
  []
  (aget js/globalThis (js/Symbol.for "open-hax.sol/module-url")))
