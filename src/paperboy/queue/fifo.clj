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
            [paperboy.queue.impl.memory :as mem]
            [paperboy.specs :as specs]
            [paperboy.utils :as utils]))

(def ^:private sender ::fifo)

(deftype FIFO [state ready removed-ready on-event on-error]
  api/Consumer
  (await-ready! [_ timeout-ms]
    (s/assert nat-int? timeout-ms)
    (mem/await! state :messages ready timeout-ms))

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
    (mem/ack! state removed-ready sender on-event on-error envelope))

  api/Producer
  (put! [_ envelope]
    (s/assert ::specs/envelope envelope)
    (let [id (get-in envelope [:message :id])]
      (locking state
        (vswap! state
                (fn [{:keys [ids] :as state'}]
                  (mem/assert-new-id! ids id)
                  (-> state'
                      (update :messages conj envelope)
                      (update :ids conj id))))
        (.release ^java.util.concurrent.Semaphore ready)))
    (ev/emit! on-event (ev/put sender envelope)))

  (await-removed! [_ timeout-ms]
    (s/assert nat-int? timeout-ms)
    (mem/await! state :removed removed-ready timeout-ms))

  (drain-removed! [_ batch-size remove-fn]
    (s/assert pos-int? batch-size)
    (s/assert fn? remove-fn)
    (mem/drain-removed! state
                         removed-ready
                         sender
                         on-event
                         on-error
                         batch-size
                         remove-fn)))

(alter-meta! #'->FIFO assoc :private true)

(defn fifo
  ([]
   (fifo {}))
  ([opts]
   (utils/validate! ::specs/event-opts "Invalid options" opts)
   (let [{:keys [on-event on-error]} opts]
     (->FIFO (volatile! {:messages mem/empty-queue
                         :in-flight {}
                         :removed #{}
                         :ids #{}})
             (java.util.concurrent.Semaphore. 0)
             (java.util.concurrent.Semaphore. 0)
             on-event
             on-error))))