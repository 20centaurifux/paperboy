(ns paperboy.event
  "Constructors and callback dispatch for Paperboy's observable events.

  Components emit events to report lifecycle changes and the progress or
  failure of message processing to the optional `:on-event` and `:on-error`
  callbacks. Events are notifications for monitoring, logging, and tests; they
  do not control the component or alter the result of an operation."
  (:require [clojure.spec.alpha :as s]
            [paperboy.specs :as specs]))

;;; Specs

;; Events

(s/def ::event keyword?)
(s/def ::sender some?)
(s/def ::data map?)
(s/def ::operation keyword?)
(s/def ::cause #(instance? Throwable %))

(s/def ::ev (s/keys :req-un [::event
                             ::sender
                             ::data]))

(s/def ::error-ev
  (s/keys :req-un [::event
                   ::sender]
          :opt-un [::operation
                   ::cause]))

;; Transition

(s/def ::from keyword?)
(s/def ::to keyword?)

(s/def ::transition
  (s/keys :req-un [::from ::to]
          :opt-un [::cause]))

;;; Create Events

(defn event
  "Creates an event describing something observed in a component.

  `event` identifies what happened, `sender` identifies the component that
  reported it, and `data` carries event-specific context. Returns a map with
  the keys `:event`, `:sender`, and `:data`."
  [event sender data]
  {:post [(s/valid? ::ev %)]}
  {:event event
   :sender sender
   :data data})

(defn error
  "Creates an error event describing a component or operation failure.

  `event` identifies the kind of failure and `sender` identifies the component
  that reported it. Copies the optional keys `:component`, `:operation`, and
  `:cause` from `error-data`; all other entries are ignored.

  Returns a map containing `:event`, `:sender`, and any copied optional keys."
  [event sender error-data]
  {:post [(s/valid? ::error-ev %)]}
  (merge (select-keys error-data [:component :operation :cause])
         {:event event
          :sender sender}))

;;; Emit Events

(defn emit!
  "Notifies an observer about `event`.

  Passes `event` to `callback-fn` when it is a function. Callback return values
  and thrown `Throwable`s are ignored so observation cannot affect component
  processing. Always returns nil."
  [callback-fn event]
  (when (fn? callback-fn)
    (try
      (callback-fn event)
      (catch Throwable _)))
  nil)

;;; Lifecycle

(defn transition
  "Reports that a component changed its lifecycle state.

  `transition` describes the change: `:from` is the previous state, `:to` is
  the new state, and the optional `:cause` explains why the change occurred,
  for example after a failure. Returns a `:lifecycle/transition` event whose
  data contains the available `:from`, `:to`, and `:cause` entries; all other
  entries are ignored."
  [sender transition]
  {:pre [(s/valid? ::transition transition)]}
  (event :lifecycle/transition
         sender
         (select-keys transition [:from :to :cause])))

;;; Queue

;; Consumer

(defn claim
  "Reports that a consumer claimed `envelope` for exclusive processing.

  The `:consumer/claim` event marks the point at which the envelope leaves the
  and becomes in flight. Its data is `{:envelope envelope}`."
  [sender envelope]
  {:pre [(s/valid? ::specs/envelope envelope)]}
  (event :consumer/claim sender {:envelope envelope}))

(defn ack
  "Reports that a consumer acknowledged successful delivery of `envelope`.

  The `:consumer/ack` event marks the envelope as ready for post-delivery
  removal. Its data is `{:envelope envelope}`."
  [sender envelope]
  {:pre [(s/valid? ::specs/envelope envelope)]}
  (event :consumer/ack sender {:envelope envelope}))

;; Producer

(defn put
  "Reports that a producer added `envelope` to a queue.

  The `:producer/put` event marks the point at which the envelope becomes
  available to consumers. Its data is `{:envelope envelope}`."
  [sender envelope]
  {:pre [(s/valid? ::specs/envelope envelope)]}
  (event :producer/put sender {:envelope envelope}))

(defn begin-drain-removed
  "Reports that a producer is about to remove acknowledged message IDs.

  The `:producer/begin-drain-removed` event is emitted before `batch` is passed
  to the external removal function. Its data is `{:batch batch}`."
  [sender batch]
  {:pre [(s/valid? (s/coll-of string? :kind set?) batch)]}
  (event :producer/begin-drain-removed sender {:batch batch}))

(defn end-drain-removed
  "Reports that acknowledged message IDs were removed successfully.

  The `:producer/end-drain-removed` event is emitted after the external removal
  function accepted `batch`. Its data is `{:batch batch}`."
  [sender batch]
  {:pre [(s/valid? (s/coll-of string? :kind set?) batch)]}
  (event :producer/end-drain-removed sender {:batch batch}))

;;; Spooler

(defn begin-submit
  "Reports that a spooler is about to submit a payload for delivery.

  The `:spooler/begin-submit` event precedes creation or queuing of the delivery.
  Its data is `{:path path :payload payload}`."
  [sender path payload]
  {:pre [(s/valid? ::specs/path path)
         (s/valid? ::specs/payload payload)]}
  (event :spooler/begin-submit
         sender
         {:path path :payload payload}))

(defn end-submit
  "Reports that a spooler submitted a payload successfully.

  The `:spooler/end-submit` event confirms that the delivery was prepared and
  handed off. Its data is `{:path path :payload payload}`."
  [sender path payload]
  {:pre [(s/valid? ::specs/path path)
         (s/valid? ::specs/payload payload)]}
  (event :spooler/end-submit
         sender {:path path :payload payload}))

;;; Broker

(defn begin-transmit
  "Reports that a broker is about to transmit `envelope`.

  The `:broker/begin-transmit` event is emitted after the envelope is claimed
  and before the delivery attempt. Its data is `{:envelope envelope}`."
  [sender envelope]
  {:pre [(s/valid? ::specs/envelope envelope)]}
  (event :broker/begin-transmit sender {:envelope envelope}))

(defn end-transmit
  "Reports that a broker transmitted `envelope` successfully.

  The `:broker/end-transmit` event is emitted after delivery and before the
  envelope is acknowledged. Its data is `{:envelope envelope}`."
  [sender envelope]
  {:pre [(s/valid? ::specs/envelope envelope)]}
  (event :broker/end-transmit sender {:envelope envelope}))

;;; Errors

(defn operation-failure
  "Creates an error event for an unsuccessful `operation`.

  The two-argument form returns `{:event :operation/failure, :sender sender,
  :operation operation}`. The three-argument form returns an
  `:operation/failed` event and includes the supplied `cause`."
  ([sender operation]
   {:pre [(s/valid? ::operation operation)]}
   (error :operation/failure sender {:operation operation}))
  ([sender operation cause]
   {:pre [(s/valid? ::operation operation)
          (s/valid? ::cause cause)]}
   (error :operation/failed sender {:operation operation
                                    :cause cause})))