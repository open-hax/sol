(ns open-hax.sol.extern.exclusive-fs
  "Clio storage filesystem boundary: exclusive create/rollback and canonical
   path resolution. Raw `node:fs` descriptors stay in this extern namespace;
   callers receive only paths and an ownership callback."
  (:require ["node:fs" :as node-fs]
            ["fs-ext-extra-prebuilt" :as fse]))

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

(defn file-identity
  "Return {:dev :ino} for an existing path, or nil if missing. Used to prove
   a published ledger is still the same inode as the staged file."
  [p]
  (try
    (let [st (.statSync node-fs (str p))]
      {:dev (.-dev st) :ino (.-ino st)})
    (catch :default _ nil)))

(defn same-file-identity?
  "True when both paths exist and name the same inode (hard links included)."
  [a b]
  (let [ia (file-identity a) ib (file-identity b)]
    (boolean (and ia ib (= ia ib)))))

(defn try-lock-file!
  "Non-blocking exclusive flock on an existing file. Returns an opaque lock
   handle, or nil when another process holds the lock (EAGAIN/EWOULDBLOCK).
   Caller must release-lock!."
  [p]
  (let [constants (.-constants fse)
        fd (.openSync node-fs (str p) "r+")]
    (try
      (.flockSync fse fd (bit-or (.-LOCK_EX constants) (.-LOCK_NB constants)))
      {:fd fd :path (str p)}
      (catch :default error
        (.closeSync node-fs fd)
        (let [code (.-code error)]
          (if (or (= "EAGAIN" code) (= "EWOULDBLOCK" code))
            nil
            (throw error)))))))

(defn lock-file!
  "Blocking exclusive flock on an existing file. Returns an opaque lock handle."
  [p]
  (let [constants (.-constants fse)
        fd (.openSync node-fs (str p) "r+")]
    (try
      (.flockSync fse fd (.-LOCK_EX constants))
      {:fd fd :path (str p)}
      (catch :default error
        (.closeSync node-fs fd)
        (throw error)))))

(defn release-lock!
  "Unlock and close a handle from lock-file! / try-lock-file!."
  [lock]
  (when lock
    (let [fd (:fd lock)
          constants (.-constants fse)]
      (try (.flockSync fse fd (.-LOCK_UN constants)) (catch :default _ nil))
      (try (.closeSync node-fs fd) (catch :default _ nil)))))
