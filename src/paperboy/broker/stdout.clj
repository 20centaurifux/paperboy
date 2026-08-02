(ns paperboy.broker.stdout
  (:require [paperboy.api :as api]))

(def ^:private ready-timeout-ms 250)

(defn- run
  [consumer]
  (try
    (while (not (.isInterrupted (Thread/currentThread)))
      (when (api/await-ready! consumer ready-timeout-ms)
        (when-let [envelope (api/claim! consumer)]
          (let [{path :path {id :id payload :payload} :message} envelope]
            (printf "Message `%s' to `%s'\n\t`%s'\n\n" id path payload)
            (flush)
            (api/ack! consumer envelope)))))
    (catch InterruptedException _)))

(deftype Stdout [consumer worker]
  api/Lifecycle
  (start! [_]
    (locking worker
      (when-not @worker
        (let [thread (Thread. ^Runnable (bound-fn [] (run consumer))
                              "paperboy-broker-stdout")]
          (reset! worker thread)
          (.start thread))))
    nil)

  (stop! [_]
    (locking worker
      (when-let [thread @worker]
        (.interrupt ^Thread thread)
        (.join ^Thread thread)
        (reset! worker nil)))
    nil))

(defn stdout
  [consumer]
  (->Stdout consumer (atom nil)))