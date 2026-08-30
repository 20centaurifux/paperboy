(ns paperboy.queue.quota
  "An in-memory queue that applies size quotas per resolved route bucket.

  The queue implements Paperboy's `Consumer` and `Producer` protocols. It
  groups waiting envelopes into quota buckets. A quota configuration contains a
  default bucket size and optional route-specific bucket sizes:

  {:size 100
   :routes {\"/tenant-a/*\" {:size 10}
            \"/tenant-b/?\" {:size 20}}}

  Quota patterns use Paperboy path syntax plus two wildcards: `?` matches one
  path segment and `*` matches the rest of the path. Patterns are translated to
  reitit routes internally. A matched envelope is bucketed by its concrete
  matched path and uses the size from the matching quota route. Unmatched paths
  go into the `\"*\"` default bucket.

  The queue state is an immutable map held in a volatile:

  `:buffers` maps bucket path to a FIFO of waiting envelopes.
  `:paths` is a FIFO of non-empty bucket paths used for round-robin claiming.
  `:in-flight` maps claimed envelope IDs to envelopes.
  `:removed` contains acknowledged or quota-evicted IDs awaiting deletion.
  `:ids` reserves every ID still owned by the queue.

  Adding to a full bucket evicts older waiting envelopes from that bucket. Their
  IDs become removable immediately; claimed envelopes still become removable
  only after `ack!`."
  (:require [clojure.spec.alpha :as s]
            [instaparse.core :as insta]
            [paperboy.api :as api]
            [paperboy.event :as ev]
            [paperboy.queue.impl.memory :as mem]
            [paperboy.specs :as specs]
            [paperboy.utils :as utils]
            [reitit.core :as reitit]))

(def ^:private sender ::quota)

;;; Specs

(s/def ::size pos-int?)
(s/def ::route-quota (s/keys :req-un [::size]))
(s/def ::routes (s/map-of string? ::route-quota))
(s/def ::quotas (s/keys :req-un [::size]
                        :opt-un [::routes]))

;;; Resolve Paths => Quotas

;; Accepts only the same path characters as `api/envelope`, so quota patterns
;; cannot accidentally describe paths the rest of Paperboy rejects.
(def ^:private parser
  (insta/parser
   "pattern = (separator (element | single-wildcard))+
               | (separator (element | single-wildcard))+ separator multi-wildcard
               | separator multi-wildcard
     separator = '/'
     element  = #'[0-9a-zA-Z_-]{1,20}'
     single-wildcard = '?'
     multi-wildcard = '*'"))

;; Transforms the `parser` result to reitit-compatible route parts.
;; `?` becomes a numbered route parameter, `*` consumes the rest of the path.
;; Numbered parameters avoid duplicate names for patterns with several `?`
;; wildcards, for example `/foo/?/?` => `/foo/:_1/:_2`.
(defn- parsed-pattern->reitit-route-parts
  [parsed]
  (-> (reduce (fn [{:keys [index] :as state} [tag value]]
                (let [part (case tag
                             :separator value
                             :element value
                             :single-wildcard (str ":_" index)
                             :multi-wildcard "*path")]
                  (cond-> (update state :parts conj part)
                    (= :single-wildcard tag) (update :index inc))))
              {:parts [] :index 1}
              (rest parsed))
      :parts))

;; Parses one quota pattern string and returns the equivalent reitit route.
;; Invalid patterns fail before the router is created, so producers never see
;; partially working quota configuration.
(defn- pattern->reitit-route
  [pattern]
  (let [parsed (parser pattern)]
    (when (insta/failure? parsed)
      (throw (ex-info "Invalid pattern"
                      {:pattern pattern
                       :result parsed})))
    (->> (parsed-pattern->reitit-route-parts parsed)
         (apply str))))

;; Converts a route-pattern -> quota map to the route table expected by reitit.
;; The quota map is stored as route data, so a later path match can return the
;; matching bucket size without another lookup.
(defn- quota-routes->reitit-routes
  [m]
  (reduce-kv (fn [result pattern quota]
               (conj result [(pattern->reitit-route pattern) quota]))
             []
             (or m {})))

;; Builds the function used by `put!` to choose a quota bucket for an envelope
;; path. Matching paths get their own concrete bucket path, but the bucket size
;; comes from the matching quota route. Unmatched paths go into the `"*"`
;; default bucket.
(defn- resolve-fn
  [quotas]
  (let [routes (quota-routes->reitit-routes (:routes quotas))
        router (reitit/router routes)]
    (fn [path]
      (if-let [match (reitit/match-by-path router path)]
        {:path (-> match :path)
         :size (-> match :data :size)}
        {:path "*"
         :size (:size quotas)}))))

;;; Queue

;; Buffer Helpers

;; Returns IDs evicted from the front and the buffer tail that still fits into
;; `max-size`. Newer envelopes stay in the buffer; older envelopes become
;; removable.
(defn- split-buffer
  [buffer max-size]
  (loop [evicted []
         remaining buffer]
    (if (> (count remaining) max-size)
      (recur (conj evicted (get-in (peek remaining) [:message :id]))
             (pop remaining))
      [evicted remaining])))

;; Missing buckets behave like empty FIFO queues.
(defn- get-buffer
  [buffers path]
  (-> buffers
      (get-in [:buffers path] mem/empty-queue)))

;; Adds an envelope to one quota bucket and trims that bucket back to its quota.
;; Evicted IDs move straight to `:removed`; they were never claimed, but the
;; producer still needs to delete their persisted messages.
(defn- append-envelope
  [buffers path envelope max-size]
  (let [new-path? (not (contains? (:buffers buffers) path))
        buffer (conj (get-buffer buffers path) envelope)
        [evicted remaining] (split-buffer buffer max-size)]
    [(cond-> (-> buffers
                 (update :removed into evicted)
                 (update :ids conj (get-in envelope [:message :id]))
                 (assoc-in [:buffers path] remaining))
       new-path?
       (update :paths conj path))
     evicted]))

;; Claims from the next active bucket. If the bucket still contains envelopes,
;; its path is moved to the back of `:paths`; otherwise the bucket disappears
;; from both `:buffers` and `:paths`. This gives active quota buckets a simple
;; round-robin order.
(defn- claim-envelope
  [buffers]
  (when-let [path (peek (:paths buffers))]
    (let [buffer (get-buffer buffers path)
          envelope (peek buffer)
          remaining (pop buffer)
          buffers' (-> buffers
                       (assoc-in [:in-flight
                                  (get-in envelope [:message :id])]
                                 envelope))]
      (if (empty? remaining)
        [(-> buffers'
             (update :buffers dissoc path)
             (update :paths pop))
         envelope]
        [(-> buffers'
             (assoc-in [:buffers path] remaining)
             (update :paths #(conj (pop %) path)))
         envelope]))))

;; `state` is a volatile containing an immutable state map. The volatile is also
;; used as the lock monitor, so state updates and semaphore updates stay in
;; sync.
(deftype Quota [state resolve ready removed-ready on-event on-error]
  api/Consumer
  (await-ready! [_ timeout-ms]
    (s/assert nat-int? timeout-ms)
    (mem/await! state :buffers ready timeout-ms))

  (claim! [_]
    (let [envelope (locking state
                     (when-let [[state' envelope] (claim-envelope @state)]
                       (vreset! state state')
                       ;; Consume one ready permit for the envelope that just
                       ;; left the waiting buffers.
                       (.tryAcquire ^java.util.concurrent.Semaphore ready)
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
    (let [id (get-in envelope [:message :id])
          {:keys [path size]} (resolve (:path envelope))]
      (locking state
        (let [{:keys [ids] :as state'} @state]
          (mem/assert-new-id! ids id)
          (let [[state'' evicted] (append-envelope state'
                                                   path
                                                   envelope
                                                   size)
                claimable-added? (empty? evicted)]
            (vreset! state state'')
            ;; Adding to a full bucket evicts one or more older
            ;; envelopes. In that case the number of claimable envelopes
            ;; did not grow, so `ready` must not receive an extra permit.
            (when claimable-added?
              (.release ^java.util.concurrent.Semaphore ready))
            ;; Evicted IDs become removable immediately. Acknowledged
            ;; IDs use the same `:removed` set and semaphore via
            ;; `mem/ack!`.
            (when (seq evicted)
              (.release ^java.util.concurrent.Semaphore
               removed-ready
                        (count evicted))))))
      (ev/emit! on-event (ev/put sender envelope))))

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

(alter-meta! #'->Quota assoc :private true)

(defn quota
  ([quotas]
   (quota quotas {}))
  ([quotas opts]
   (utils/validate! ::quotas "Invalid quotas" quotas)
   (utils/validate! ::specs/event-opts "Invalid options" opts)
   (let [{:keys [on-event on-error]} opts]
     (->Quota (volatile! {:buffers {}
                          :in-flight {}
                          :removed #{}
                          :paths mem/empty-queue
                          :ids #{}})
              (resolve-fn quotas)
              (java.util.concurrent.Semaphore. 0)
              (java.util.concurrent.Semaphore. 0)
              on-event
              on-error))))