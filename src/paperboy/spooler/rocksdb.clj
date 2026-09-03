(ns paperboy.spooler.rocksdb
  "A persistent spooler backed by RocksDB.

   Envelopes are persisted before they are put on the queue. When a spooler is
   constructed, every persisted envelope is restored to the queue in ascending
   message-ID order. The greatest restored ID initializes an atom used to
   allocate unique, monotonically increasing IDs for subsequent submissions. If
   restoration fails, envelopes already put on the queue are not rolled back.

   A background component removes acknowledged message IDs from RocksDB. If a
   queue put fails after persistence, the envelope remains in RocksDB and is
   restored when a new spooler instance is constructed with  the same database."
  (:require [clj-rocksdb :as rocks]
            [clojure.edn :as edn]
            [paperboy.api :as api]
            [paperboy.event :as ev]
            [paperboy.specs :as specs]
            [paperboy.utils :as utils]
            [supervise.core :as sv])
  (:import [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(def ^:private purge-batch-size 100)
(def ^:private cleanup-interval-ms 5000)
(def ^:private sender ::rocksdb)

(defn- encode-id [id]
  (.array (doto (ByteBuffer/allocate Long/BYTES)
            (.putLong (long id)))))

(defn- decode-id [bytes]
  (.getLong (ByteBuffer/wrap bytes)))

(defn- encode-value [value]
  (.getBytes (pr-str value) StandardCharsets/UTF_8))

(defn- decode-value [^bytes bytes]
  (edn/read-string (String. bytes StandardCharsets/UTF_8)))

(defn- open-db [directory]
  (rocks/create-db directory
                   {:key-encoder encode-id
                    :key-decoder decode-id
                    :val-encoder encode-value
                    :val-decoder decode-value}))

(defn- stored->envelope [id {:keys [path payload]}]
  (api/envelope path {:id (str id) :payload payload}))

(defn- restore! [db queue]
  (with-open [^java.io.Closeable entries (rocks/iterator db)]
    (reduce (fn [_ [id stored]]
              (api/put! queue (stored->envelope id stored))
              id)
            0
            entries)))

(defn- delete-messages! [db ids]
  (apply rocks/delete db (map parse-long ids))
  (rocks/sync db)
  true)

(defn- component [db queue {:keys [on-event]}]
  (sv/component
   (fn [interrupt-token]
     (loop []
       (sv/check! interrupt-token)
       (api/await-removed! queue cleanup-interval-ms)
       (api/drain-removed! queue
                           purge-batch-size
                           (fn [batch]
                             (delete-messages! db batch)))
       (recur)))
   {:on-transit (fn [transition]
                  (ev/emit! on-event (ev/transition sender transition)))
    :restart-after-failure? true}))

(deftype RocksDBSpooler [queue db seq-no component opts closed?]
  java.io.Closeable
  (close [_]
    (locking closed?
      (when-not @closed?
        (try
          (sv/stop! component)
          (finally
            (vreset! closed? true)
            (.close ^java.io.Closeable db))))))

  api/Lifecycle
  (start! [_]
    (locking closed?
      (when @closed?
        (throw (ex-info "RocksDB spooler is closed" {})))
      (sv/start! component)))

  (stop! [_]
    (sv/stop! component))

  api/Spooler
  (submit! [_ path payload]
    (locking closed?
      (when @closed?
        (throw (ex-info "RocksDB spooler is closed" {})))
      (let [{:keys [on-event on-error]} opts
            id (swap! seq-no #(Math/incrementExact (long %)))
            envelope (api/envelope path {:id (str id) :payload payload})]
        (ev/emit! on-event (ev/begin-submit sender path payload))
        (try
          (rocks/put db id {:path path :payload payload})
          (rocks/sync db)
          (api/put! queue envelope)
          (ev/emit! on-event (ev/end-submit sender path payload))
          envelope
          (catch Exception exception
            (ev/emit! on-error (ev/operation-failure sender
                                                     :spooler/submit
                                                     exception))
            (throw exception)))))))

(alter-meta! #'->RocksDBSpooler assoc :private true)

(defn rocksdb
  "Creates a closeable persistent spooler backed by the RocksDB database at
  `directory`.

  Construction opens the database, restores all persisted envelopes to `queue`
  in ascending message-ID order, and initializes the next message ID from the
  greatest ID that is still persisted. IDs are unique among messages retained
  by the spooler, but are not globally monotonic across restarts: once higher
  IDs have been removed, they may be assigned again after a restart. Messages
  may be submitted immediately after construction.

  `start!` and `stop!` control the background cleanup worker. Stopping the worker
  does not prevent submissions, and the worker may subsequently be started again.

  Calling `close` stops the cleanup worker and permanently closes the database.
  A closed spooler cannot be started or used for further submissions.

  Event callbacks can be supplied as `:on-event` and `:on-error` options."
  ([directory queue]
   (rocksdb directory queue {}))
  ([directory queue opts]
   (utils/validate! ::specs/non-blank-string "Invalid directory" directory)
   (utils/validate! ::specs/event-opts "Invalid options" opts)
   (let [db (open-db directory)]
     (try
       (let [seq-no (restore! db queue)]
         (->RocksDBSpooler queue
                           db
                           (atom seq-no)
                           (component db queue opts)
                           opts
                           (volatile! false)))
       (catch Throwable e
         (.close ^java.io.Closeable db)
         (throw e))))))