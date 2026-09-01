(ns paperboy.broker.stdout-test
  (:require [clojure.test :refer [deftest is testing]]
            [paperboy.api :as api]
            [paperboy.broker.stdout :as stdout]
            [paperboy.test-utils :as test-utils]))

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

(deftest stdout-test
  (let [broker (stdout/stdout (oneshot-consumer nil (promise)))]
    (is (satisfies? api/Lifecycle broker)))

  (testing "rejects invalid event callbacks"
    (doseq [opts [{:on-event :not-a-function}
                  {:on-error :not-a-function}]]
      (let [exception (test-utils/thrown-ex-info
                       #(stdout/stdout (oneshot-consumer nil (promise)) opts))]
        (is (some? exception))
        (is (= "Invalid options" (ex-message exception)))
        (is (= opts (:value (ex-data exception))))))))

(deftest lifecycle-test
  (let [transitioned (promise)
        events (atom [])
        broker (stdout/stdout
                (oneshot-consumer nil (promise))
                {:on-event (fn [event]
                             (let [events' (swap! events conj event)]
                               (when (= 4 (count events'))
                                 (deliver transitioned events'))))})]
    (api/start! broker)
    (api/stop! broker)

    (is (= [{:event :lifecycle/transition
             :sender :paperboy.broker.stdout/stdout
             :data {:from :stopped :to :starting}}
            {:event :lifecycle/transition
             :sender :paperboy.broker.stdout/stdout
             :data {:from :starting :to :running}}
            {:event :lifecycle/transition
             :sender :paperboy.broker.stdout/stdout
             :data {:from :running :to :stopping}}
            {:event :lifecycle/transition
             :sender :paperboy.broker.stdout/stdout
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
        broker (stdout/stdout consumer)]
    (try
      (api/start! broker)
      (is (= 500 (deref awaited 2000 ::timeout)))
      (finally
        (api/stop! broker)))))

(deftest transmit-test
  (let [acknowledged (promise)
        events (atom [])
        output (java.io.StringWriter.)
        broker (stdout/stdout (oneshot-consumer envelope acknowledged)
                              {:on-event #(swap! events conj %)})]
    (try
      (with-redefs [*out* output]
        (api/start! broker)
        (is (= envelope (deref acknowledged 2000 ::timeout))))
      (finally
        (api/stop! broker)))

    (testing "writes and flushes the envelope"
      (is (= "Message `message-1' to `/notifications/email'\n\t`hello'\n"
             (str output))))

    (testing "emits begin and end events"
      (is (= [{:event :broker/begin-transmit
               :sender :paperboy.broker.stdout/stdout
               :data {:envelope envelope}}
              {:event :broker/end-transmit
               :sender :paperboy.broker.stdout/stdout
               :data {:envelope envelope}}]
             (broker-events @events))))))

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
        broker (stdout/stdout consumer)]
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
        broker (stdout/stdout (oneshot-consumer nil acknowledged)
                              {:on-event #(swap! events conj %)})]
    (try
      (api/start! broker)
      (is (= ::timeout (deref acknowledged 100 ::timeout)))
      (finally
        (api/stop! broker)))
    (is (empty? (broker-events @events)))))

(deftest consumer-operation-failure-test
  (doseq [operation [:await-ready :claim]]
    (testing (name operation)
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
            broker (stdout/stdout
                    consumer
                    {:on-event (fn [event]
                                 (swap! events conj event)
                                 (when (= :failed (-> event :data :to))
                                   (deliver failed event)))
                     :on-error #(swap! errors conj %)})]
        (try
          (api/start! broker)
          (let [failure-event (deref failed 2000 ::timeout)]
            (is (= {:from :running :to :failed}
                   (select-keys (:data failure-event) [:from :to])))
            (is (identical? failure (-> failure-event :data :cause))))
          (finally
            (api/stop! broker)))

        (is (empty? (broker-events @events)))
        ;; Queue-operation failures are reported through the queue
        ;; implementation's on-error callback, not through the broker's.
        (is (empty? @errors))))))

(deftest acknowledgement-failure-test
  (let [failure (ex-info "acknowledgement failed" {:queue :test})
        pending (atom envelope)
        failed (promise)
        events (atom [])
        errors (atom [])
        output (java.io.StringWriter.)
        consumer (reify api/Consumer
                   (await-ready! [_ _]
                     true)
                   (claim! [_]
                     (let [claimed @pending]
                       (reset! pending nil)
                       claimed))
                   (ack! [_ _]
                     (throw failure)))
        broker (stdout/stdout
                consumer
                {:on-event (fn [event]
                             (swap! events conj event)
                             (when (= :failed (-> event :data :to))
                               (deliver failed event)))
                 :on-error #(swap! errors conj %)})]
    (try
      (with-redefs [*out* output]
        (api/start! broker)
        (let [failure-event (deref failed 2000 ::timeout)]
          (is (= {:from :running :to :failed}
                 (select-keys (:data failure-event) [:from :to])))
          (is (identical? failure (-> failure-event :data :cause)))))
      (finally
        (api/stop! broker)))

    (testing "writes the envelope and emits its end event before acknowledging"
      (is (= "Message `message-1' to `/notifications/email'\n\t`hello'\n"
             (str output)))
      (is (= [:broker/begin-transmit :broker/end-transmit]
             (mapv :event (broker-events @events)))))

    (testing "does not report an acknowledgement failure as a transmit failure"
      (is (empty? @errors)))))

(deftest output-failure-test
  (let [acknowledged (promise)
        reported (promise)
        failed (promise)
        events (atom [])
        failing-writer (proxy [java.io.Writer] []
                         (write [_ _ _]
                           (throw (java.io.IOException. "output closed")))
                         (flush [])
                         (close []))
        output (java.io.PrintWriter. failing-writer)
        broker (stdout/stdout (oneshot-consumer envelope acknowledged)
                              {:on-event (fn [event]
                                           (swap! events conj event)
                                           (when (= :failed (-> event :data :to))
                                             (deliver failed event)))
                               :on-error #(deliver reported %)})]
    (try
      (with-redefs [*out* output]
        (api/start! broker)
        (let [error-event (deref reported 2000 ::timeout)
              failure-event (deref failed 2000 ::timeout)]
          (is (not= ::timeout error-event))
          (is (= :operation/failed (:event error-event)))
          (is (= :paperboy.broker.stdout/stdout (:sender error-event)))
          (is (= :broker/transmit (:operation error-event)))
          (is (= "Printing to stdout failed"
                 (ex-message (:cause error-event))))
          (is (= envelope (:envelope (ex-data (:cause error-event)))))
          (is (= {:from :running :to :failed}
                 (select-keys (:data failure-event) [:from :to])))
          (is (identical? (:cause error-event)
                          (-> failure-event :data :cause)))))
      (finally
        (api/stop! broker)))

    (testing "does not acknowledge or emit an end event"
      (is (= ::timeout (deref acknowledged 100 ::timeout)))
      (is (= [:broker/begin-transmit]
             (mapv :event (broker-events @events)))))))