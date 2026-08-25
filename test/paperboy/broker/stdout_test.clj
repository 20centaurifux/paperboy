(ns paperboy.broker.stdout-test
  (:require [clojure.test :refer [deftest is testing]]
            [paperboy.api :as api]
            [paperboy.broker.stdout :as stdout]
            [paperboy.test-utils :as test-utils]))

(def envelope (api/envelope "/notifications/email"
                            {:id "message-1" :payload "hello"}))

(defn- consumer
  [envelope acknowledged]
  (let [pending (atom envelope)]
    (reify api/Consumer
      (await-ready! [_ _]
        ;; Returning true makes the broker check its interrupt token on every
        ;; iteration, allowing the supervised worker to stop promptly.
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
  (let [broker (stdout/stdout (consumer nil (promise)))]
    (is (satisfies? api/Lifecycle broker)))

  (testing "rejects invalid event callbacks"
    (doseq [opts [{:on-event :not-a-function}
                  {:on-error :not-a-function}]]
      (let [exception (test-utils/thrown-ex-info
                       #(stdout/stdout (consumer nil (promise)) opts))]
        (is (some? exception))
        (is (= "Invalid options" (ex-message exception)))
        (is (= opts (:value (ex-data exception))))))))

(deftest transmit-test
  (let [acknowledged (promise)
        events (atom [])
        output (java.io.StringWriter.)
        broker (stdout/stdout (consumer envelope acknowledged)
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

    (testing "emits begin and end events before acknowledging"
      (is (= [{:event :broker/begin-transmit
               :sender :paperboy.broker.stdout/stdout
               :data {:envelope envelope}}
              {:event :broker/end-transmit
               :sender :paperboy.broker.stdout/stdout
               :data {:envelope envelope}}]
             (broker-events @events))))))

(deftest no-envelope-test
  (let [acknowledged (promise)
        events (atom [])
        broker (stdout/stdout (consumer nil acknowledged)
                              {:on-event #(swap! events conj %)})]
    (try
      (api/start! broker)
      (is (= ::timeout (deref acknowledged 100 ::timeout)))
      (finally
        (api/stop! broker)))
    (is (empty? (broker-events @events)))))

(deftest output-failure-test
  (let [acknowledged (promise)
        reported (promise)
        events (atom [])
        failing-writer (proxy [java.io.Writer] []
                         (write [_ _ _]
                           (throw (java.io.IOException. "output closed")))
                         (flush [])
                         (close []))
        output (java.io.PrintWriter. failing-writer)
        broker (stdout/stdout (consumer envelope acknowledged)
                              {:on-event #(swap! events conj %)
                               :on-error #(deliver reported %)})]
    (try
      (with-redefs [*out* output]
        (api/start! broker)
        (let [error-event (deref reported 2000 ::timeout)]
          (is (not= ::timeout error-event))
          (is (= :operation/failed (:event error-event)))
          (is (= :paperboy.broker.stdout/stdout (:sender error-event)))
          (is (= :broker/transmit (:operation error-event)))
          (is (= "Printing to stdout failed"
                 (ex-message (:cause error-event))))
          (is (= envelope (:envelope (ex-data (:cause error-event)))))))
      (finally
        (api/stop! broker)))

    (testing "does not acknowledge or emit an end event"
      (is (= ::timeout (deref acknowledged 100 ::timeout)))
      (is (= [:broker/begin-transmit]
             (mapv :event (broker-events @events)))))))