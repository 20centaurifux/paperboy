(ns paperboy.spooler.passthrough
  "A spooler that wraps submitted payloads in envelopes and puts them directly
  onto a queue.

  A background component removes the IDs of acknowledged messages from the
  queue. Payloads are not persisted outside the queue."
  (:require [paperboy.api :as api]
            [paperboy.event :as ev]
            [paperboy.specs :as specs]
            [paperboy.utils :as utils]
            [supervise.core :as sv]))

(def ^:private purge-batch-size 100)
(def ^:private cleanup-interval-ms 5000)
(def ^:private sender ::passthrough)

(deftype Passthrough [seq-no queue component opts]
  api/Lifecycle
  (start! [_]
    (sv/start! component))

  (stop! [_]
    (sv/stop! component))

  api/Spooler
  (submit! [_ path payload]
    (let [{:keys [on-event on-error]} opts
          env (api/envelope path {:id (str (swap! seq-no inc))
                                  :payload payload})]
      (ev/emit! on-event (ev/begin-submit sender path payload))
      (try
        (api/put! queue env)
        (ev/emit! on-event (ev/end-submit sender path payload))
        (catch Exception exception
          (ev/emit! on-error (ev/operation-failure sender
                                                   :spooler/submit
                                                   exception))
          (throw exception)))
      env)))

(alter-meta! #'->Passthrough assoc :private true)

(defn- component
  [queue {:keys [on-event]}]
  (sv/component
   (fn [interrupt-token]
     (loop []
       (sv/check! interrupt-token)
       (api/await-removed! queue cleanup-interval-ms)
       (api/drain-removed! queue purge-batch-size (constantly true))
       (recur)))
   {:on-transit (fn [transition]
                  (ev/emit! on-event (ev/transition sender
                                                    transition)))
    :restart-after-failure? true}))

(defn passthrough
  ([queue]
   (passthrough queue {}))
  ([queue opts]
   (utils/validate! ::specs/event-opts "Invalid options" opts)
   (->Passthrough (atom 0)
                  queue
                  (component queue opts)
                  opts)))