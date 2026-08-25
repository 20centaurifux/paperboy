(ns paperboy.queue.fifo
  "An in-memory FIFO queue implementing Paperboy's `Consumer` and `Producer`
  protocols.

  The queue state is an immutable map held in a volatile. `:messages` is a
  persistent FIFO queue, `:in-flight` holds envelopes that have been claimed
  but not yet acknowledged, and `:removed` contains acknowledged IDs awaiting
  external deletion. The `:ids` set reserves every ID still owned by the queue.
  An ID moves from `:messages` to `:in-flight`, then to `:removed`; it is
  released from `:ids` only after its removal function succeeds.

  The volatile also serves as the monitor for `locking`. State transitions and
  their corresponding semaphore updates share this lock so their counts cannot
  diverge. `ready` represents queued envelopes and `removed-ready` represents
  IDs available for removal. Waiting happens outside the lock to avoid blocking.

  A drain call atomically takes all currently removable IDs and processes its
  private snapshot in batches without holding the lock. This makes concurrent
  drain calls safe: they can only take IDs acknowledged after an earlier
  snapshot. Successful batches release their reserved IDs. On a falsy result or
  exception, the failed batch and all unattempted batches are merged back into
  `:removed` and their permits are restored."
  (:require [clojure.spec.alpha :as s]
            [paperboy.api :as api]
            [paperboy.event :as ev]
            [paperboy.specs :as specs]
            [paperboy.utils :as utils]))

(def ^:private sender ::fifo)

(defn- await!
  [state state-key ready timeout-ms]
  ;; Waiting while holding the lock would prevent producers from releasing a
  ;; permit.
  (if (.tryAcquire ^java.util.concurrent.Semaphore ready
                   timeout-ms
                   java.util.concurrent.TimeUnit/MILLISECONDS)
    (locking state
      ;; Recheck the state under the lock and restore the acquired permit
      ;; only while work remains; otherwise it would become a stale signal.
      (if (seq (get @state state-key))
        (do
          (.release ^java.util.concurrent.Semaphore ready)
          true)
        false))
    false))

(deftype FIFO [state ready removed-ready on-event on-error]
  api/Consumer
  (await-ready! [_ timeout-ms]
    (s/assert nat-int? timeout-ms)
    (await! state :messages ready timeout-ms))

  (claim! [_]
    (let [envelope
          (locking state
            (let [{:keys [messages] :as state'} @state
                  envelope (peek messages)]
              (when envelope
                (vreset! state
                         (-> state'
                             (assoc :messages (pop messages))
                             (assoc-in [:in-flight
                                        (get-in envelope [:message :id])]
                                       envelope)))
                (.tryAcquire ^java.util.concurrent.Semaphore ready))
              envelope))]
      (when envelope
        (ev/emit! on-event (ev/claim sender envelope)))
      envelope))

  (ack! [_ envelope]
    (s/assert ::specs/envelope envelope)
    (let [id (get-in envelope [:message :id])
          acknowledged? (locking state
                          (let [{:keys [in-flight] :as state'} @state
                                acknowledged? (contains? in-flight id)]
                            (when acknowledged?
                              (vreset! state
                                       (-> state'
                                           (update :in-flight dissoc id)
                                           (update :removed conj id)))
                              (.release ^java.util.concurrent.Semaphore removed-ready))
                            acknowledged?))]
      (if acknowledged?
        (ev/emit! on-event (ev/ack sender envelope))
        (ev/emit! on-error (ev/operation-failure sender
                                                 :consumer/ack
                                                 (ex-info "Envelope not found"
                                                          {:envelope envelope}))))))

  api/Producer
  (put! [_ envelope]
    (s/assert ::specs/envelope envelope)
    (let [id (get-in envelope [:message :id])]
      (locking state
        (vswap! state
                (fn [{:keys [ids] :as state'}]
                  (when (contains? ids id)
                    (throw (ex-info "Message ID must be unique" {:id id})))
                  (-> state'
                      (update :messages conj envelope)
                      (update :ids conj id))))
        (.release ^java.util.concurrent.Semaphore ready)))
    (ev/emit! on-event (ev/put sender envelope)))

  (await-removed! [_ timeout-ms]
    (s/assert nat-int? timeout-ms)
    (await! state :removed removed-ready timeout-ms))

  (drain-removed! [_ batch-size remove-fn]
    (s/assert pos-int? batch-size)
    (s/assert fn? remove-fn)
    ;; Claim all pending IDs at once so concurrent drainers cannot process the
    ;; same IDs. Acknowledgements added afterwards remain in :removed.
    (let [removed (locking state
                    (let [state' @state
                          removed (:removed state')]
                      (vreset! state (assoc state' :removed #{}))
                      (dotimes [_ (count removed)]
                        (.tryAcquire ^java.util.concurrent.Semaphore removed-ready))
                      removed))]
      (loop [batches (seq (partition-all batch-size removed))
             drained #{}]
        (if-let [pending (first batches)]
          (let [batch (set pending)
                ;; Run external code without holding the state lock so queue
                ;; operations and other drainers can continue concurrently.
                result (try
                         (ev/emit! on-event (ev/begin-drain-removed sender batch))
                         (remove-fn batch)
                         (catch Throwable throwable
                           (ev/emit! sender
                                     (ev/operation-failure :producer/drain-removed
                                                           throwable))
                           false))]
            (if result
              (do
                ;; Successful IDs leave the queue lifecycle.
                (locking state
                  (vswap! state update :ids #(apply disj % batch)))
                (ev/emit! on-event (ev/end-drain-removed sender batch))
                (recur (next batches) (into drained batch)))
              (let [remaining (set (mapcat identity batches))]
                ;; Restore the failed batch and every batch not attempted yet.
                (locking state
                  (vswap! state update :removed into remaining)
                  (.release ^java.util.concurrent.Semaphore removed-ready
                            (count remaining)))
                drained)))
          drained)))))

(alter-meta! #'->FIFO assoc :private true)

(defn fifo
  ([]
   (fifo {}))
  ([opts]
   (utils/validate! ::specs/event-opts "Invalid options" opts)
   (let [{:keys [on-event on-error]} opts]
     (->FIFO (volatile! {:messages clojure.lang.PersistentQueue/EMPTY
                         :in-flight {}
                         :removed #{}
                         :ids #{}})
             (java.util.concurrent.Semaphore. 0)
             (java.util.concurrent.Semaphore. 0)
             on-event
             on-error))))