(ns paperboy.api
  (:require [paperboy.specs :as specs]
            [paperboy.utils :as utils]))

;;; Envelope

(defrecord Envelope [path message])

(defn envelope
  [path message]
  (utils/validate! ::specs/path "Invalid path" path)
  (utils/validate! ::specs/message "Invalid message" message)
  (->Envelope path message))

;;; Protocols

(defprotocol Lifecycle
  (start! [component])
  (stop! [component]))

(defprotocol Consumer
  (await-ready! [consumer timeout-ms])
  (claim! [consumer])
  (ack! [consumer envelope]))

(defprotocol Producer
  (put! [producer envelope])
  (await-removed! [producer timeout-ms])
  (drain-removed! [producer batch-size remove-fn]))

(defprotocol Spooler
  (submit! [spooler path payload]))