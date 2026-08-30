(ns paperboy.queue.impl.memory
  "Shared helpers for Paperboy's in-memory queue implementations.

  This namespace is intentionally specific to the state shape used by the
  current in-memory queues. It owns the common message-ID lifecycle around
  `:in-flight`, `:removed`, and `:ids`, plus the semaphore handling needed for
  waiting consumers and removal drainers.

  Queue implementations still define their own waiting-buffer structure, for
  example FIFO's single `:messages` queue or Quota's per-bucket buffers. They
  call these helpers once an envelope has entered or left that local waiting
  structure."
  (:require [paperboy.event :as ev]))

(def empty-queue
  clojure.lang.PersistentQueue/EMPTY)

(defn assert-new-id!
  "Throws when `id` is already reserved in `ids`.

  Queue implementations call this before accepting a new envelope so message
  IDs stay unique while they are queued, in flight, acknowledged, or waiting for
  removal."
  [ids id]
  (when (contains? ids id)
    (throw (ex-info "Message ID must be unique" {:id id}))))

(defn await!
  "Waits up to `timeout-ms` for work signalled by `ready`.

  `state-key` names the collection in the queue `state` that must still contain
  work after a permit was acquired. Returns true when work is available, false
  on timeout or when the acquired permit turned out to be stale."
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

(defn ack!
  "Acknowledges the in-flight envelope with the same message ID as `envelope`.

  A successful acknowledgement is ID-based: it moves the matching stored
  envelope from `:in-flight` to `:removed`, releases `removed-ready`, and emits
  an ack event for that stored envelope. Unknown IDs are left untouched and
  reported via `on-error`."
  [state removed-ready sender on-event on-error envelope]
  (let [id (get-in envelope [:message :id])
        match (locking state
                (let [{:keys [in-flight] :as state'} @state]
                  (when-let [match' (in-flight id)]
                    (vreset! state
                             (-> state'
                                 (update :in-flight dissoc id)
                                 (update :removed conj id)))
                    (.release ^java.util.concurrent.Semaphore
                     removed-ready)
                    match')))]
    (if match
      (ev/emit! on-event (ev/ack sender match))
      (ev/emit! on-error (ev/operation-failure sender
                                               :consumer/ack
                                               (ex-info "Envelope not found"
                                                        {:envelope envelope}))))))

(defn drain-removed!
  "Drains removable message IDs in batches accepted by `remove-fn`.

  The current `:removed` set is claimed atomically before calling `remove-fn`.
  Each successful batch releases those IDs from `:ids`. If `remove-fn` returns
  falsy or throws, the failed and unattempted IDs are restored to `:removed`.
  Returns the set of IDs successfully drained by this call."
  [state removed-ready sender on-event on-error batch-size remove-fn]
  ;; Claim all pending IDs at once so concurrent drainers cannot process the
  ;; same IDs. Acknowledgements added afterwards remain in :removed.
  (let [removed (locking state
                  (let [state' @state
                        removed' (:removed state')]
                    (vreset! state (assoc state' :removed #{}))
                    (dotimes [_ (count removed')]
                      (.tryAcquire ^java.util.concurrent.Semaphore
                       removed-ready))
                    removed'))]
    (loop [batches (seq (partition-all batch-size removed))
           drained #{}]
      (if-let [pending (first batches)]
        (let [batch (set pending)
              ;; Run external code without holding the state lock so queue
              ;; operations and other drainers can continue concurrently.
              result (try
                       (ev/emit! on-event
                                 (ev/begin-drain-removed sender batch))
                       (remove-fn batch)
                       (catch Throwable throwable
                         (ev/emit! on-error
                                   (ev/operation-failure
                                    sender
                                    :producer/drain-removed
                                    throwable))
                         false))]
          (if result
            (do
              ;; Successful IDs leave the queue lifecycle.
              (locking state
                (vswap! state update :ids #(apply disj % batch)))
              (ev/emit! on-event (ev/end-drain-removed sender batch))
              (recur (next batches) (into drained batch)))
            (let [remaining (into #{} (mapcat identity) batches)]
              ;; Restore the failed batch and every batch not attempted yet.
              (locking state
                (vswap! state update :removed into remaining)
                (.release ^java.util.concurrent.Semaphore removed-ready
                          (count remaining)))
              drained)))
        drained))))
