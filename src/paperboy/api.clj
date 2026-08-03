(ns paperboy.api
  "Public types, constructors, and component protocols for Paperboy."
  (:require [paperboy.specs :as specs]
            [paperboy.utils :as utils]))

;;; Envelope

(defrecord ^{:doc "A message together with the logical destination to which it is delivered."}
 Envelope [path message])

(alter-meta! #'->Envelope assoc :private true)
(alter-meta! #'map->Envelope assoc :private true)

(defn envelope
  "Creates an envelope for `message` addressed to `path`.

  The path must start with a slash and consist of one or more slash-separated
  segments containing letters, digits, underscores, or hyphens. Each segment
  may contain at most 20 characters. The message must contain non-blank string
  values for `:id` and `:payload`.

  Throws `ExceptionInfo` when the path or message is invalid."
  [path message]
  (utils/validate! ::specs/path "Invalid path" path)
  (utils/validate! ::specs/message "Invalid message" message)
  (->Envelope path message))

;;; Protocols

(defprotocol Lifecycle
  "Controls the runtime lifecycle of a Paperboy component."
  (start! [component]
    "Starts `component`. Has no effect if it is already running.")
  (stop! [component]
    "Stops `component`. Has no effect if it is not running."))

(defprotocol Consumer
  "Provides the consumer side of a Paperboy queue."
  (await-ready! [consumer timeout-ms]
    "Waits up to `timeout-ms` milliseconds for an envelope to become available.
    Returns true if an envelope becomes available before the timeout, otherwise
    false.")
  (claim! [consumer]
    "Claims and returns the next available envelope, or nil when none is
    available. A claimed envelope is withheld from other consumers until it is
    acknowledged.")
  (ack! [consumer envelope]
    "Acknowledges the successful delivery of a previously claimed `envelope`."))

(defprotocol Producer
  "Provides the producer and post-delivery cleanup side of a Paperboy queue."
  (put! [producer envelope]
    "Adds `envelope` to the queue and makes it available to consumers.")
  (await-removed! [producer timeout-ms]
    "Waits up to `timeout-ms` milliseconds for acknowledged message IDs to
    become ready for removal. Returns true if IDs become available before the
    timeout, otherwise false.")
  (drain-removed! [producer batch-size remove-fn]
    "Passes acknowledged message IDs to `remove-fn` in sets of at most
    `batch-size`. IDs are drained only for batches for which `remove-fn` returns
    truthy. Returns the set of IDs drained by this call."))

(defprotocol Spooler
  "Accepts messages and prepares them for delivery."
  (submit! [spooler path payload]
    "Submits `payload` for delivery to `path` and returns its envelope."))