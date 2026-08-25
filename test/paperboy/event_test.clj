(ns paperboy.event-test
  (:require [clojure.test :refer [deftest is testing]]
            [paperboy.api :as api]
            [paperboy.event :as event]))

(def sender ::component)
(def envelope (api/envelope "/notifications" {:id "message-1"
                                              :payload "hello"}))

(deftest event-test
  (is (= {:event :message/received
          :sender sender
          :data {:message-id "message-1"}}
         (event/event :message/received sender {:message-id "message-1"})))

  (testing "invalid arguments"
    (is (thrown? AssertionError (event/event "received" sender {})))
    (is (thrown? AssertionError (event/event :message/received nil {})))
    (is (thrown? AssertionError
                 (event/event :message/received sender "message-1")))))

(deftest error-test
  (let [cause (ex-info "delivery failed" {})]
    (is (= {:event :delivery/failed
            :sender sender
            :component :broker
            :operation :broker/transmit
            :cause cause}
           (event/error :delivery/failed
                        sender
                        {:component :broker
                         :operation :broker/transmit
                         :cause cause
                         :ignored true}))))

  (testing "invalid arguments"
    (is (thrown? AssertionError (event/error "failed" sender {})))
    (is (thrown? AssertionError (event/error :delivery/failed nil {})))
    (is (thrown? AssertionError
                 (event/error :delivery/failed sender {:operation "transmit"})))
    (is (thrown? AssertionError
                 (event/error :delivery/failed sender {:cause "failure"})))))

(deftest emit-test
  (testing "passes the event to a callback and returns nil"
    (let [received (atom [])
          event {:event :message/received}]
      (is (nil? (event/emit! #(swap! received conj %) event)))
      (is (= [event] @received))))

  (testing "ignores missing and non-function callbacks"
    (is (nil? (event/emit! nil ::event)))
    (is (nil? (event/emit! :not-a-function ::event))))

  (testing "suppresses throwables raised by callbacks"
    (is (nil? (event/emit! #(throw (AssertionError. "observer failed"))
                           ::event)))))

(deftest transition-test
  (let [cause (ex-info "component failed" {})]
    (is (= {:event :lifecycle/transition
            :sender sender
            :data {:from :running
                   :to :failed
                   :cause cause}}
           (event/transition sender {:from :running
                                     :to :failed
                                     :cause cause
                                     :ignored true}))))

  (testing "invalid arguments"
    (doseq [transition [nil
                        {}
                        {:from :stopped}
                        {:to :running}
                        {:from "stopped" :to :running}
                        {:from :running :to "stopped"}
                        {:from :running :to :failed :cause "failure"}]]
      (testing (str "rejects " (pr-str transition))
        (is (thrown? AssertionError (event/transition sender transition)))))))

(deftest envelope-event-test
  (doseq [[constructor event-name]
          [[event/claim :consumer/claim]
           [event/ack :consumer/ack]
           [event/put :producer/put]
           [event/begin-transmit :broker/begin-transmit]
           [event/end-transmit :broker/end-transmit]]]
    (testing (name event-name)
      (is (= {:event event-name
              :sender sender
              :data {:envelope envelope}}
             (constructor sender envelope)))
      (is (thrown? AssertionError (constructor sender {}))))))

(deftest drain-removed-event-test
  (let [batch #{"message-1" "message-2"}]
    (doseq [[constructor event-name]
            [[event/begin-drain-removed :producer/begin-drain-removed]
             [event/end-drain-removed :producer/end-drain-removed]]]
      (testing (name event-name)
        (is (= {:event event-name
                :sender sender
                :data {:batch batch}}
               (constructor sender batch)))
        (is (thrown? AssertionError (constructor sender ["message-1"])))
        (is (thrown? AssertionError (constructor sender #{:message-1})))))))

(deftest submit-event-test
  (doseq [[ctor event-name]
          [[event/begin-submit :spooler/begin-submit]
           [event/end-submit :spooler/end-submit]]]
    (testing (name event-name)
      (is (= {:event event-name
              :sender sender
              :data {:path "/notifications"
                     :payload "hello"}}
             (ctor sender "/notifications" "hello")))
      (is (thrown? AssertionError
                   (ctor sender "notifications" "hello")))
      (is (thrown? AssertionError
                   (ctor sender "/notifications" " "))))))

(deftest operation-failure-test
  (testing "without cause"
    (is (= {:event :operation/failure
            :sender sender
            :operation :broker/transmit}
           (event/operation-failure sender :broker/transmit))))

  (testing "with cause"
    (let [cause (ex-info "transmit failed" {})]
      (is (= {:event :operation/failed
              :sender sender
              :operation :broker/transmit
              :cause cause}
             (event/operation-failure sender :broker/transmit cause)))))

  (testing "invalid arguments"
    (is (thrown? AssertionError
                 (event/operation-failure sender "broker/transmit")))
    (is (thrown? AssertionError
                 (event/operation-failure sender :broker/transmit "failure")))))