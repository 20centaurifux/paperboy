(ns paperboy.queue.fifo
  (:require [paperboy.api :as api]))

(deftype FIFO [state ready removed-ready]
  api/Consumer
  (await-ready! [_ timeout-ms]
    (.tryAcquire ^java.util.concurrent.Semaphore ready
                 timeout-ms
                 java.util.concurrent.TimeUnit/MILLISECONDS))

  (claim! [_]
    (let [[before _]
          (swap-vals! state
                      (fn [{:keys [messages] :as current}]
                        (if-let [envelope (peek messages)]
                          (-> current
                              (assoc :messages (pop messages))
                              (assoc-in [:in-flight (get-in envelope [:message :id])]
                                        envelope))
                          current)))]
      (peek (:messages before))))

  (ack! [_ envelope]
    (let [id (get-in envelope [:message :id])
          [before _]
          (swap-vals! state
                      (fn [{:keys [in-flight] :as current}]
                        (if (contains? in-flight id)
                          (-> current
                              (update :in-flight dissoc id)
                              (update :removed conj id))
                          current)))]
      (when (contains? (:in-flight before) id)
        (.release ^java.util.concurrent.Semaphore removed-ready)))
    nil)

  api/Producer
  (put! [_ envelope]
    (swap! state update :messages conj envelope)
    (.release ^java.util.concurrent.Semaphore ready)
    nil)

  (await-removed! [_ timeout-ms]
    (if (.tryAcquire ^java.util.concurrent.Semaphore removed-ready
                     timeout-ms
                     java.util.concurrent.TimeUnit/MILLISECONDS)
      (do
        (.drainPermits ^java.util.concurrent.Semaphore removed-ready)
        true)
      false))

  (drain-removed! [_ batch-size remove-fn]
    (when-not (pos-int? batch-size)
      (throw (ex-info "Batch size must be a positive integer"
                      {:batch-size batch-size})))

    (loop [batches (seq (partition-all batch-size (:removed @state)))
           drained #{}]
      (if-let [pending (first batches)]
        (let [batch (set pending)]
          (if (remove-fn batch)
            (do
              (swap! state update :removed #(apply disj % batch))
              (recur (next batches) (into drained batch)))
            drained))
        drained))))

(defn fifo
  []
  (->FIFO (atom {:messages clojure.lang.PersistentQueue/EMPTY
                 :in-flight {}
                 :removed #{}})
          (java.util.concurrent.Semaphore. 0)
          (java.util.concurrent.Semaphore. 0)))