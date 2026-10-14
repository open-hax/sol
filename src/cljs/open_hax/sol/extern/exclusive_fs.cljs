(ns open-hax.sol.extern.exclusive-fs
  "Clio storage filesystem boundary: exclusive create/rollback and canonical
   path resolution. Raw `node:fs` descriptors stay in this extern namespace;
   callers receive only paths and an ownership callback."
  (:require ["node:fs" :as node-fs]))

(defn realpath
  "Resolve symlinks to the canonical absolute path. Throws if `p` is missing."
  [p]
  (.realpathSync node-fs (str p)))

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

(defn- delete-if-file!
  [p]
  (when (.existsSync node-fs (str p))
    (let [st (.lstatSync node-fs (str p))]
      (when (.isFile st)
        (.unlinkSync node-fs (str p))))))

(defn directory-empty?
  "True when p exists as a directory with no entries."
  [p]
  (and (.existsSync node-fs (str p))
       (.isDirectory (.lstatSync node-fs (str p)))
       (zero? (alength (.readdirSync node-fs (str p))))))

(defn empty-regular-file?
  "True when p exists as a regular file of size 0. Used so initialization
   rollback does not unlink a ledger another process has already appended to."
  [p]
  (boolean
   (when (.existsSync node-fs (str p))
     (let [st (.lstatSync node-fs (str p))]
       (and (.isFile st) (zero? (.-size st)))))))

(defn rollback-owned-directory!
  "Remove only `owned-files` under p, then the directory itself when empty.
   Other children — nested directories, concurrent ledger files, or Clio's own
   `<root>.edn.tmp-<uuid>` snapshots from a concurrent `runtime/open` — are
   left in place. Clio deletes its own temporary schema file in `finally`."
  [p owned-files]
  (let [root (str p)]
    (doseq [owned owned-files
            :let [target (str owned)]]
      (delete-if-file! target))
    (when (.existsSync node-fs root)
      (try
        (.rmdirSync node-fs root)
        (catch :default _ nil))))
  p)
