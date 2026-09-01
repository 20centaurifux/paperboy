(ns paperboy.broker.mh
  "An MQTT broker implementation backed by Machine Head.
  
   The broker claims and publishes one envelope at a time. It keeps the claimed
   envelope in flight until publishing and acknowledgement succeed, preserving
   message order and providing backpressure while MQTT is unavailable.
  
   If publishing succeeds but acknowledgement fails, the envelope is published
   again. Delivery is therefore at least once, and MQTT consumers must tolerate
   duplicate messages."
  (:require [clojurewerkz.machine-head.client :as mh]
            [paperboy.api :as api]
            [paperboy.event :as ev]
            [paperboy.specs :as specs]
            [paperboy.utils :as utils]
            [supervise.core :as sv]))

(def ^:private ready-timeout-ms 500)
(def ^:private transmit-in-flight-delay-ms 1500)
(def ^:private sender ::mqtt)

(deftype Mqtt [component]
  api/Lifecycle
  (start! [_]
    (sv/start! component))

  (stop! [_]
    (sv/stop! component)))

(alter-meta! #'->Mqtt assoc :private true)

(defn- component
  [client consumer {:keys [on-event on-error]}]
  (sv/component
   (fn [interrupt-token]
     (loop [in-flight nil]
       (sv/check! interrupt-token)
       ;; Retry the current envelope before claiming another one.
       (if-let [envelope (or in-flight
                             (and (api/await-ready! consumer ready-timeout-ms)
                                  (api/claim! consumer)))]
         (let [in-flight' (if (mh/connected? client)
                            (try
                              (ev/emit! on-event (ev/begin-transmit sender envelope))
                              (mh/publish client
                                          (get envelope :path)
                                          (get-in envelope [:message :payload]))
                              (ev/emit! on-event (ev/end-transmit sender envelope))
                              (api/ack! consumer envelope)
                              ;; Clear the in-flight slot after successful delivery.
                              nil
                              (catch Exception exception
                                (ev/emit! on-error
                                          (ev/operation-failure sender
                                                                :broker/transmit
                                                                exception))
                                envelope))
                            ;; Retain the claimed envelope while MQTT is disconnected.
                            envelope)]
           (when in-flight'
             (sv/await! interrupt-token transmit-in-flight-delay-ms))
           (recur in-flight'))
         (recur nil))))
   {:on-transit (fn [transition]
                  (ev/emit! on-event (ev/transition sender
                                                    transition)))}))

(defn mqtt
  ([consumer client]
   (mqtt consumer client {}))
  ([consumer client opts]
   (utils/validate! ::specs/event-opts "Invalid options" opts)
   (->Mqtt (component client consumer opts))))