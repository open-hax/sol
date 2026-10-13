(ns open-hax.sol.extern.exclusive-fs
  "Exclusive-create boundary for Clio storage initialization. Raw `node:fs`
   descriptors stay in this extern namespace; callers receive only paths and
   an ownership callback."
  (:require ["node:fs" :as node-fs]))

(defn create-file!
  "Create an empty file with exclusive (`wx`) semantics, as Clio's
   `create-ledger!` does. `on-created` runs after the exclusive open succeeds
   and before the descriptor is closed, so the caller owns cleanup even when
   the close fails. An exclusive-open failure (for example EEXIST) throws
   without calling `on-created`. Returns p."
  [p on-created]
  (let [fd (.openSync node-fs (str p) "wx")]
    (try
      (on-created)
      (finally
        (.closeSync node-fs fd))))
  p)

(defn create-directory!
  "Create one directory exclusively (non-recursive `mkdir`): the parent must
   already exist and an existing directory throws EEXIST without calling
   `on-created`. `on-created` runs only once this call has created the
   directory, so the caller can roll back exactly what it owns. Returns p."
  [p on-created]
  (.mkdirSync node-fs (str p))
  (on-created)
  p)
