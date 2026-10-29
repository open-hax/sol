(ns open-hax.sol.extern.module-url
  "Stable module-owned anchor for Sol's own install (ESM builds).
   Closure cannot compile import.meta, so each ESM module entry in
   shadow-cljs.edn :prepends a module-scoped const holding its
   import.meta.url, and also records it under a registry symbol; this
   namespace only reads them. The CommonJS test build aliases this namespace
   to module-url-cjs.")

(defn module-url
  "URL (file://...) of the loaded Sol module file, or nil.
   Release output shares one module scope with the prepend, so each bundle
   reads its own const and two imported Sol bundles keep separate anchors.
   Unoptimized output loads every namespace as a separate module that cannot
   see that const; it falls back to the shared registry, which is safe there
   because a second unoptimized Sol bundle cannot load into the same realm
   (its goog namespaces are global and already declared)."
  []
  (if (exists? js/open_hax$sol$module_url)
    js/open_hax$sol$module_url
    (aget js/globalThis (js/Symbol.for "open-hax.sol/module-url"))))
