(ns open-hax.sol.extern.exclusive-fs
  "Clio storage filesystem boundary: exclusive create/rollback and canonical
   path resolution. Raw `node:fs` descriptors stay in this extern namespace;
   callers receive only paths and an ownership callback."
  (:require ["node:fs" :as node-fs]
            ["node:path" :as node-path]
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
   already exist and an existing path (even an empty directory) fails without
   calling `on-created`. EEXIST is shaped as `{:exclusive-fs/error
   :already-exists}` so callers never read Node `.code`. `on-created` runs
   only once this call has created the directory, so the caller can roll back
   exactly what it owns. Returns p."
  [p on-created]
  (try
    (.mkdirSync node-fs (str p))
    (catch :default error
      (if (= "EEXIST" (.-code error))
        (throw (ex-info "Exclusive directory target already exists"
                        {:exclusive-fs/error :already-exists
                         :path (str p)}))
        (throw error))))
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
   a published ledger is still the same inode as the staged file. Read as
   bigint stats and kept as decimal strings, so 64-bit device/inode values
   above Number.MAX_SAFE_INTEGER never round into a false match."
  [p]
  (try
    (let [st (.statSync node-fs (str p) #js {:bigint true})]
      {:dev (str (.-dev st)) :ino (str (.-ino st))})
    (catch :default _ nil)))

(defn same-file-identity?
  "True when both paths exist and name the same inode (hard links included)."
  [a b]
  (let [ia (file-identity a) ib (file-identity b)]
    (boolean (and ia ib (= ia ib)))))

(defn try-lock-file!
  "Non-blocking exclusive flock on the lock file at p, creating it (never
   truncating) when missing, so a lock path never fails ENOENT. Returns an
   opaque lock handle, or nil when another holder has the lock
   (EAGAIN/EWOULDBLOCK). Caller must release-lock!."
  [p]
  (let [constants (.-constants fse)
        fd (.openSync node-fs (str p) "a")]
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
  "Blocking exclusive flock on the lock file at p, creating it (never
   truncating) when missing. Lock files are permanent: deleting one would let
   a later opener lock a different inode than a current holder. Returns an
   opaque lock handle; caller must release-lock!."
  [p]
  (let [constants (.-constants fse)
        fd (.openSync node-fs (str p) "a")]
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


(defn- fsync-directory!
  "Best-effort durability for a rename inside dir (POSIX); hosts that cannot
   open a directory skip it."
  [dir]
  (try
    (let [fd (.openSync node-fs (str dir) "r")]
      (try (.fsyncSync node-fs fd) (finally (.closeSync node-fs fd))))
    (catch :default _ nil)))

(defn replace-file!
  "Crash-atomically set p to the complete `text`: write and fsync a private
   sibling `<p>.tmp`, then rename it over p. A reader (or a restart after
   process death) sees either the previous complete file or the new one,
   never an empty or partial body. A leftover `<p>.tmp` from an earlier death
   is discarded first; callers serialize writers with an external lock.
   Returns p."
  [p text]
  (let [tmp (str p ".tmp")]
    (try (.unlinkSync node-fs tmp) (catch :default _ nil))
    (write-new-file! tmp text (fn [] nil))
    (.renameSync node-fs tmp (str p))
    (fsync-directory! (.dirname node-path (str p))))
  p)

(defn unlink-if-empty!
  "Unpublish p only while its inode is still empty. p is first hard-linked
   to a private `<p>.unpublish-<uuid>` name and unlinked, so the inode stays
   reachable; if it then holds any bytes (another opener appended), it is
   linked back to p and kept. Returns :removed, :kept, :kept-aside (p was
   re-created meanwhile; the bytes stay at the private name) or :missing."
  [p]
  (let [hold (str p ".unpublish-" (random-uuid))]
    (if-not (try (.linkSync node-fs (str p) hold) true
                 (catch :default error
                   (if (= "ENOENT" (.-code error)) false (throw error))))
      :missing
      (do
        (.unlinkSync node-fs (str p))
        (if (pos? (.-size (.statSync node-fs hold)))
          (if (try (.linkSync node-fs hold (str p)) true
                   (catch :default error
                     (if (= "EEXIST" (.-code error)) false (throw error))))
            (do (.unlinkSync node-fs hold) :kept)
            :kept-aside)
          (do (.unlinkSync node-fs hold) :removed))))))
