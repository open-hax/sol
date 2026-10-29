(ns open-hax.sol.extern.module-url-cjs
  "CommonJS twin of open-hax.sol.extern.module-url (test build alias).")

(defn module-url
  "Path of the loaded Sol module file, when the CommonJS loader exposes it."
  []
  (when (exists? js/__filename) js/__filename))
