(ns open-hax.sol.extern.exclusive-fs
  "Exclusive-create boundary for Clio storage initialization. Raw `node:fs`
   descriptors stay in this extern namespace; callers receive only paths and
   an ownership callback."
  (:require ["node:fs" :as node-fs]
            ["node:path" :as node-path]))

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

(defn rollback-owned-directory!
  "Remove regular files under p that this initializer may have written, then
   the directory itself only when empty. Nested directories are left in place
   so a concurrent claim beneath p is not deleted."
  [p]
  (let [root (str p)]
    (when (.existsSync node-fs root)
      (doseq [name (.readdirSync node-fs root)]
        (let [child (.join node-path root name)
              st (.lstatSync node-fs child)]
          (when (.isFile st)
            (.unlinkSync node-fs child))))
      (try
        (.rmdirSync node-fs root)
        (catch :default _ nil))))
  p)
