(ns paperboy.spooler.passthrough-test
  (:require [clojure.test :refer [deftest is testing]]
            [paperboy.api :as api]
            [paperboy.spooler.passthrough :as passthrough]
            [paperboy.test-utils :as test-utils]))

(defn- producer
  [put-fn]
  (reify api/Producer
    (put! [_ envelope]
      (put-fn envelope))
    (await-removed! [_ _]
      false)
    (drain-removed! [_ _ _]
      #{})))

(deftest passthrough-test
  (let [queue (producer identity)
        spooler (passthrough/passthrough queue)]
    (is (satisfies? api/Spooler spooler))
    (is (satisfies? api/Lifecycle spooler)))

  (testing "rejects invalid event callbacks"
    (doseq [opts [{:on-event :not-a-function}
                  {:on-error :not-a-function}]]
      (let [exception (test-utils/thrown-ex-info
                       #(passthrough/passthrough (producer identity) opts))]
        (is (some? exception))
        (is (= "Invalid options" (ex-message exception)))
        (is (= opts (:value (ex-data exception))))))))

(deftest submit-test
  (let [envelopes (atom [])
        events (atom [])
        spooler (passthrough/passthrough
                 (producer #(swap! envelopes conj %))
                 {:on-event #(swap! events conj %)})
        first-envelope (api/submit! spooler "/notifications/email" "hello")
        second-envelope (api/submit! spooler "/notifications/sms" "world")]
    (testing "creates sequentially numbered envelopes and puts them on the queue"
      (is (= {:path "/notifications/email"
              :message {:id "1" :payload "hello"}}
             (into {} first-envelope)))
      (is (= {:path "/notifications/sms"
              :message {:id "2" :payload "world"}}
             (into {} second-envelope)))
      (is (= [first-envelope second-envelope] @envelopes)))

    (testing "emits begin and end events in submission order"
      (is (= [{:event :spooler/begin-submit
               :sender :paperboy.spooler.passthrough/passtrough
               :data {:path "/notifications/email" :payload "hello"}}
              {:event :spooler/end-submit
               :sender :paperboy.spooler.passthrough/passtrough
               :data {:path "/notifications/email" :payload "hello"}}
              {:event :spooler/begin-submit
               :sender :paperboy.spooler.passthrough/passtrough
               :data {:path "/notifications/sms" :payload "world"}}
              {:event :spooler/end-submit
               :sender :paperboy.spooler.passthrough/passtrough
               :data {:path "/notifications/sms" :payload "world"}}]
             @events)))))

(deftest submit-validation-test
  (let [put-count (atom 0)
        events (atom [])
        spooler (passthrough/passthrough
                 (producer (fn [_] (swap! put-count inc)))
                 {:on-event #(swap! events conj %)})]
    (doseq [[path payload] [["notifications" "hello"]
                            ["/notifications" " "]]]
      (is (thrown? clojure.lang.ExceptionInfo
                   (api/submit! spooler path payload))))
    (is (zero? @put-count))
    (is (empty? @events))))

(deftest submit-queue-failure-test
  (let [failure (ex-info "queue unavailable" {:queue :test})
        events (atom [])
        errors (atom [])
        spooler (passthrough/passthrough
                 (producer (fn [_] (throw failure)))
                 {:on-event #(swap! events conj %)
                  :on-error #(swap! errors conj %)})
        thrown (test-utils/thrown-ex-info
                #(api/submit! spooler "/notifications" "hello"))]
    (testing "rethrows the queue exception"
      (is (identical? failure thrown)))

    (testing "emits begin and failure events, but no end event"
      (is (= :spooler/begin-submit (-> @events first :event)))
      (is (= {:event :operation/failed
              :sender :paperboy.spooler.passthrough/passtrough
              :operation :spooler/submit
              :cause failure}
             (first @errors))))))