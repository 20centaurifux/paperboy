(ns paperboy.spooler.passthrough
  (:require [paperboy.api :as api]))

(def ^:private purge-batch-size 100)
(def ^:private cleanup-interval-ms 5000)

(defn- run
  [queue]
  (try
    (while (not (.isInterrupted (Thread/currentThread)))
      (api/await-removed! queue cleanup-interval-ms)
      (api/drain-removed! queue purge-batch-size (constantly true)))
    (catch InterruptedException _)))

(deftype Passthrough [seq-no queue worker]
  api/Lifecycle
  (start! [_]
    (locking worker
      (when-not @worker
        (let [thread (Thread. ^Runnable (bound-fn [] (run queue))
                              "paperboy-spooler-passthrough")]
          (reset! worker thread)
          (.start thread))))
    nil)

  (stop! [_]
    (locking worker
      (when-let [thread @worker]
        (.interrupt ^Thread thread)
        (.join ^Thread thread)
        (reset! worker nil)))
    nil)

  api/Spooler
  (submit! [_ path payload]
    (let [env (api/envelope path {:id (str (swap! seq-no inc))
                                  :payload payload})]
      (api/put! queue env)
      env)))

(defn passthrough
  [queue]
  (->Passthrough (atom 0) queue (atom nil)))