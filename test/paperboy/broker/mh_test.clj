(ns paperboy.broker.mh-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojurewerkz.machine-head.client :as mh]
            [paperboy.api :as api]
            [paperboy.broker.mh :as mqtt]
            [paperboy.test-utils :as test-utils]
            [paperboy.support.mqtt-broker :as mqtt-broker]))

(def ^:private default-mqtt-port 11883)

(def ^:dynamic *mqtt-broker* nil)
(def ^:dynamic *mqtt-client* nil)

(defn- mqtt-port
  []
  (if-let [port (System/getenv "PAPERBOY_MQTT_PORT")]
    (Integer/parseInt port)
    default-mqtt-port))

(defn- with-mqtt-broker
  [test-fn]
  (binding [*mqtt-broker* (mqtt-broker/broker (mqtt-port))]
    (try
      (mqtt-broker/start! *mqtt-broker*)
      (binding [*mqtt-client* (mh/connect (format "tcp://127.0.0.1:%d" (mqtt-port))
                                          {:opts {:auto-reconnect true}})]
        (try
          (test-fn)
          (finally
            (mh/disconnect-and-close *mqtt-client*))))
      (finally
        (mqtt-broker/stop! *mqtt-broker*)))))

(use-fixtures :once with-mqtt-broker)

(defn- await-connected?
  [client]
  (let [connected? (promise)]
    (future
      (loop []
        (Thread/sleep 50)
        (if (mh/connected? client)
          (deliver connected? true)
          (recur))))
    connected?))

(def ^:private envelope (api/envelope "/notifications/email"
                                      {:id "message-1" :payload "hello"}))

(defn- oneshot-consumer
  [envelope acknowledged]
  (let [pending (atom envelope)]
    (reify api/Consumer
      (await-ready! [_ _]
        true)
      (claim! [_]
        (let [claimed @pending]
          (reset! pending nil)
          claimed))
      (ack! [_ claimed]
        (deliver acknowledged claimed)))))

(defn- broker-events
  [events]
  (filterv #(= "broker" (namespace (:event %))) events))

(deftest mqtt-test
  (let [broker (mqtt/mqtt (oneshot-consumer nil (promise)) *mqtt-client*)]
    (is (satisfies? api/Lifecycle broker)))

  (testing "rejects invalid event callbacks"
    (doseq [opts [{:on-event :not-a-function}
                  {:on-error :not-a-function}]]
      (let [exception (test-utils/thrown-ex-info
                       #(mqtt/mqtt (oneshot-consumer nil (promise))
                                   *mqtt-client*
                                   opts))]
        (is (some? exception))
        (is (= "Invalid options" (ex-message exception)))
        (is (= opts (:value (ex-data exception))))))))

(deftest lifecycle-test
  (let [transitioned (promise)
        events (atom [])
        broker (mqtt/mqtt
                (oneshot-consumer nil (promise))
                *mqtt-client*
                {:on-event (fn [event]
                             (let [events' (swap! events conj event)]
                               (when (= 4 (count events'))
                                 (deliver transitioned events'))))})]
    (api/start! broker)
    (api/stop! broker)

    (is (= [{:event :lifecycle/transition
             :sender :paperboy.broker.mh/mqtt
             :data {:from :stopped :to :starting}}
            {:event :lifecycle/transition
             :sender :paperboy.broker.mh/mqtt
             :data {:from :starting :to :running}}
            {:event :lifecycle/transition
             :sender :paperboy.broker.mh/mqtt
             :data {:from :running :to :stopping}}
            {:event :lifecycle/transition
             :sender :paperboy.broker.mh/mqtt
             :data {:from :stopping :to :stopped}}]
           (deref transitioned 2000 ::timeout)))))

(deftest ready-timeout-test
  (let [awaited (promise)
        consumer (reify api/Consumer
                   (await-ready! [_ timeout-ms]
                     (deliver awaited timeout-ms)
                     false)
                   (claim! [_]
                     nil)
                   (ack! [_ _]))
        broker (mqtt/mqtt consumer *mqtt-client*)]
    (try
      (api/start! broker)
      (is (= 500 (deref awaited 2000 ::timeout)))
      (finally
        (api/stop! broker)))))

(deftest transmit-test
  (let [received (promise)
        acknowledged (promise)
        events (atom [])
        broker (mqtt/mqtt (oneshot-consumer envelope acknowledged)
                          *mqtt-client*
                          {:on-event #(swap! events conj %)})]
    (mh/subscribe *mqtt-client*
                  {(:path envelope) 0}
                  (fn [topic _metadata payload]
                    (deliver received {:topic topic
                                       :payload (String. ^bytes payload "UTF-8")})))
    (try
      (api/start! broker)

      (testing "receive MQTT message"
        (is (= {:topic (:path envelope) :payload "hello"}
               (deref received 2000 ::timeout))))

      (testing "acknowledge envelope"
        (is (= envelope (deref acknowledged 2000 ::timeout))))

      (finally
        (api/stop! broker)
        (mh/unsubscribe *mqtt-client* [(:path envelope)])))

    (testing "emits begin and end events"
      (is (= [{:event :broker/begin-transmit
               :sender :paperboy.broker.mh/mqtt
               :data {:envelope envelope}}
              {:event :broker/end-transmit
               :sender :paperboy.broker.mh/mqtt
               :data {:envelope envelope}}]
             (broker-events @events))))))

(deftest transmit-after-reconnect-test
  (let [claimed (promise)
        acknowledged (promise)
        received (promise)
        pending (atom envelope)
        consumer (reify api/Consumer
                   (await-ready! [_ _]
                     (some? @pending))
                   (claim! [_]
                     (let [claimed-envelope @pending]
                       (reset! pending nil)
                       (deliver claimed claimed-envelope)
                       claimed-envelope))
                   (ack! [_ claimed-envelope]
                     (deliver acknowledged claimed-envelope)))
        broker (mqtt/mqtt consumer *mqtt-client*)]
    (try
      (mqtt-broker/stop! *mqtt-broker*)
      (api/start! broker)

      (testing "claim envelope"
        (is (= envelope (deref claimed 2000 ::timeout))))

      (testing "reconnect MQTT client"
        (mqtt-broker/start! *mqtt-broker*)
        (is (= true (deref (await-connected? *mqtt-client*) 15000 ::timeout))))

      (testing "receive MQTT message"
        (mh/subscribe *mqtt-client*
                      {(:path envelope) 0}
                      (fn [topic _metadata payload]
                        (deliver received {:topic topic
                                           :payload (String. ^bytes payload "UTF-8")})))
        (try
          (is (= {:topic (:path envelope) :payload "hello"}
                 (deref received 2000 ::timeout)))
          (finally
            (mh/unsubscribe *mqtt-client* [(:path envelope)]))))

      (testing "acknowledge envelope"
        (is (= envelope (deref acknowledged 2000 ::timeout))))

      (finally
        (api/stop! broker)))))

(deftest transmit-retry-after-publish-failure-test
  (let [failure (ex-info "MQTT publish failed" {})
        publish-count (atom 0)
        acknowledged (promise)
        reported-error (promise)
        broker (mqtt/mqtt
                (oneshot-consumer envelope acknowledged)
                *mqtt-client*
                {:on-error #(deliver reported-error %)})]
    (with-redefs [mh/publish
                  (fn [_client _topic _payload]
                    (when (= 1 (swap! publish-count inc))
                      (throw failure)))]
      (try
        (api/start! broker)

        (testing "reports the publish failure"
          (let [error (deref reported-error 2000 ::timeout)]
            (is (= :operation/failed (:event error)))
            (is (= :paperboy.broker.mh/mqtt (:sender error)))
            (is (= :broker/transmit (:operation error)))
            (is (identical? failure (:cause error)))))

        (testing "does not acknowledge the failed attempt"
          (is (= ::timeout
                 (deref acknowledged 500 ::timeout))))

        (testing "retries and acknowledges after successful publish"
          (is (= envelope
                 (deref acknowledged 3000 ::timeout)))
          (is (= 2 @publish-count)))

        (finally
          (api/stop! broker))))))

(deftest acknowledgement-failure-republishes-test
  (let [failure (ex-info "acknowledgement failed" {:queue :test})
        pending (atom envelope)
        publish-count (atom 0)
        published-twice (promise)
        ack-count (atom 0)
        acknowledged (promise)
        reported (promise)
        consumer (reify api/Consumer
                   (await-ready! [_ _]
                     (some? @pending))
                   (claim! [_]
                     (let [claimed @pending]
                       (reset! pending nil)
                       claimed))
                   (ack! [_ acknowledged-envelope]
                     (if (= 1 (swap! ack-count inc))
                       (throw failure)
                       (deliver acknowledged acknowledged-envelope))))
        broker (mqtt/mqtt consumer
                          *mqtt-client*
                          {:on-error #(deliver reported %)})
        publish mh/publish]
    (try
      (with-redefs [mh/publish (fn [& args]
                                 (let [result (apply publish args)]
                                   (when (= 2 (swap! publish-count inc))
                                     (deliver published-twice true))
                                   result))]
        (api/start! broker)

        (testing "reports the acknowledgement failure as a transmit failure"
          (let [error-event (deref reported 2000 ::timeout)]
            (is (= :operation/failed (:event error-event)))
            (is (= :paperboy.broker.mh/mqtt (:sender error-event)))
            (is (= :broker/transmit (:operation error-event)))
            (is (identical? failure (:cause error-event)))))

        (testing "publishes the envelope again before retrying acknowledgement"
          ;; Acknowledgement is part of the transmit try/catch. Consequently,
          ;; its failure retains the envelope in flight and can duplicate it.
          (is (= true (deref published-twice 5000 ::timeout)))
          (is (= envelope (deref acknowledged 2000 ::timeout)))
          (is (= 2 @publish-count))
          (is (= 2 @ack-count))))
      (finally
        (api/stop! broker)))))

(deftest consumer-operation-failure-test
  (doseq [operation [:await-ready :claim]]
    (testing (str "failure in " (name operation))
      (let [failure (ex-info "consumer operation failed"
                             {:operation operation})
            failed (promise)
            events (atom [])
            errors (atom [])
            consumer (reify api/Consumer
                       (await-ready! [_ _]
                         (if (= :await-ready operation)
                           (throw failure)
                           true))
                       (claim! [_]
                         (if (= :claim operation)
                           (throw failure)
                           nil))
                       (ack! [_ _]))
            broker (mqtt/mqtt
                    consumer
                    *mqtt-client*
                    {:on-event
                     (fn [event]
                       (swap! events conj event)
                       (when (= :failed (get-in event [:data :to]))
                         (deliver failed event)))
                     :on-error #(swap! errors conj %)})]
        (try
          (api/start! broker)

          (let [failure-event (deref failed 2000 ::timeout)]
            (is (not= ::timeout failure-event))

            (is (= {:from :running
                    :to :failed}
                   (select-keys (:data failure-event)
                                [:from :to])))

            (is (identical? failure
                            (get-in failure-event [:data :cause]))))

          (finally
            (api/stop! broker)))

        (testing "does not report the consumer failure as MQTT transmit failure"
          (is (empty? @errors)))

        (testing "does not emit broker transmission events"
          (is (empty? (broker-events @events))))))))

(deftest not-ready-test
  (let [waiting-again (promise)
        await-count (atom 0)
        claim-count (atom 0)
        consumer (reify api/Consumer
                   (await-ready! [_ _]
                     (if (= 1 (swap! await-count inc))
                       false
                       (do
                         (deliver waiting-again true)
                         false)))
                   (claim! [_]
                     (swap! claim-count inc)
                     nil)
                   (ack! [_ _]))
        broker (mqtt/mqtt consumer *mqtt-client*)]
    (try
      (api/start! broker)
      (is (= true (deref waiting-again 2000 ::timeout)))
      (finally
        (api/stop! broker)))

    (testing "does not claim when no envelope is ready"
      (is (zero? @claim-count)))))

(deftest no-envelope-test
  (let [acknowledged (promise)
        events (atom [])
        broker (mqtt/mqtt (oneshot-consumer nil acknowledged)
                          *mqtt-client*
                          {:on-event #(swap! events conj %)})]
    (try
      (api/start! broker)
      (is (= ::timeout (deref acknowledged 100 ::timeout)))
      (finally
        (api/stop! broker)))

    (is (empty? (broker-events @events)))))