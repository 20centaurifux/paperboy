(ns paperboy.broker.stdout
  "A diagnostic broker that writes queued messages to `*out*`.

  Successfully written envelopes are acknowledged. A write failure moves the
  broker to `:failed` and leaves the claimed envelope in flight."
  (:require [paperboy.api :as api]
            [paperboy.event :as ev]
            [paperboy.specs :as specs]
            [paperboy.utils :as utils]
            [supervise.core :as sv]))

(def ^:private ready-timeout-ms 500)
(def ^:private sender ::stdout)

(deftype Stdout [component]
  api/Lifecycle
  (start! [_]
    (sv/start! component))

  (stop! [_]
    (sv/stop! component)))

(alter-meta! #'->Stdout assoc :private true)

(defn- print-envelope!
  [envelope]
  (let [{path :path
         {id :id payload :payload} :message} envelope
        ^java.io.Writer out *out*]
    (.write out (format "Message `%s' to `%s'\n\t`%s'\n" id path payload))
    (.flush out)
    (when (and (instance? java.io.PrintWriter out)
               (.checkError ^java.io.PrintWriter out))
      (throw (ex-info "Printing to stdout failed"
                      {:envelope envelope})))))

(defn- component
  [consumer {:keys [on-event on-error]}]
  (sv/component
   (fn [interrupt-token]
     (loop []
       (sv/check! interrupt-token)
       (when (api/await-ready! consumer ready-timeout-ms)
         (when-let [envelope (api/claim! consumer)]
           (ev/emit! on-event (ev/begin-transmit sender envelope))
           (try
             (print-envelope! envelope)
             (catch Exception exception
               (ev/emit! on-error
                         (ev/operation-failure sender
                                               :broker/transmit
                                               exception))
               (throw exception)))
           (ev/emit! on-event (ev/end-transmit sender envelope))
           (api/ack! consumer envelope)))
       (recur)))
   {:on-transit (fn [transition]
                  (ev/emit! on-event (ev/transition sender
                                                    transition)))}))

(defn stdout
  ([consumer]
   (stdout consumer {}))
  ([consumer opts]
   (utils/validate! ::specs/event-opts "Invalid options" opts)
   (->Stdout (component consumer opts))))