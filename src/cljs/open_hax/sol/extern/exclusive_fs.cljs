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

(defn link-new!
  "Publish `from` at `to` with exclusive semantics: a hard link (`link(2)`)
   fails rather than replacing a file another process already owns there,
   unlike rename. Both paths name the same inode afterwards. EEXIST is
   classified here as shaped Clojure data (`{:exclusive-fs/error
   :already-exists}`) so callers above extern never read Node `.code`.
   Other host failures are rethrown unchanged. Returns to."
  [from to]
  (try
    (.linkSync node-fs (str from) (str to))
    (catch :default error
      (if (= "EEXIST" (.-code error))
        (throw (ex-info "Exclusive link target already exists"
                        {:exclusive-fs/error :already-exists
                         :path (str to)}))
        (throw error))))
  to)

(defn write-new-file!
  "Write `text` to a new file at p with exclusive (`wx`) semantics. Never
   creates parent directories: a vanished parent fails ENOENT instead of being
   recreated. `writeSync` may write fewer bytes than asked, so this loops until
   every byte is written before fsync and close; a truncated file is never
   left for a caller to publish. `on-created` runs once the exclusive open
   succeeds, so the caller owns p even if the write fails."
  [p text on-created]
  (let [fd (.openSync node-fs (str p) "wx")]
    (try
      (on-created)
      (let [bytes (.from js/Buffer (str text) "utf8")
            total (.-length bytes)]
        (loop [offset 0]
          (when (< offset total)
            (let [written (.writeSync node-fs fd bytes offset (- total offset))]
              (when-not (pos? written)
                (throw (ex-info "File write made no progress" {:path (str p) :offset offset})))
              (recur (+ offset written))))))
      (.fsyncSync node-fs fd)
      (finally
        (.closeSync node-fs fd))))
  p)

(defn remove-directory-if-empty!
  "Release a claimed directory with non-recursive `rmdir`: it succeeds only
   when the directory is empty, atomically, so any child another process
   placed there (nested history, a foreign ledger, a temp snapshot) keeps the
   directory and its contents in place. Returns p."
  [p]
  (try
    (.rmdirSync node-fs (str p))
    (catch :default _ nil))
  p)
