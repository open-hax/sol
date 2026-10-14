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

(defn link-new!
  "Publish `from` at `to` with exclusive semantics: a hard link (`link(2)`)
   fails EEXIST rather than replacing a file another process already owns
   there, unlike rename. Both paths name the same inode afterwards. Returns to."
  [from to]
  (.linkSync node-fs (str from) (str to))
  to)

(defn write-new-file!
  "Write `text` to a new file at p with exclusive (`wx`) semantics. Never
   creates parent directories: a vanished parent fails ENOENT instead of being
   recreated. The caller owns p once this returns or throws after the open."
  [p text on-created]
  (let [fd (.openSync node-fs (str p) "wx")]
    (try
      (on-created)
      (.writeSync node-fs fd (str text))
      (.fsyncSync node-fs fd)
      (finally
        (.closeSync node-fs fd))))
  p)

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

