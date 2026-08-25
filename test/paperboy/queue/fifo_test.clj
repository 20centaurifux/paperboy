(ns paperboy.queue.fifo-test
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest is testing]]
            [paperboy.api :as api]
            [paperboy.queue.fifo :as fifo]
            [paperboy.test-utils :as test-utils]))

(defn- envelope
  [id]
  (api/envelope "/notifications" {:id id :payload (str "payload-" id)}))

(defn- enqueue-and-ack!
  [queue envelopes]
  (doseq [envelope envelopes]
    (api/put! queue envelope))
  (doseq [_ envelopes]
    (api/ack! queue (api/claim! queue))))

(deftest fifo-test
  (let [queue (fifo/fifo)]
    (is (satisfies? api/Producer queue))
    (is (satisfies? api/Consumer queue)))

  (testing "rejects invalid event callbacks"
    (doseq [opts [{:on-event :not-a-function}
                  {:on-error :not-a-function}]]
      (let [exception (test-utils/thrown-ex-info #(fifo/fifo opts))]
        (is (some? exception))
        (is (= "Invalid options" (ex-message exception)))
        (is (= opts (:value (ex-data exception))))))))

(deftest fifo-order-test
  (let [events (atom [])
        queue (fifo/fifo {:on-event #(swap! events conj %)})
        envelopes (mapv envelope ["1" "2" "3"])]
    (testing "an empty queue has no ready or claimable envelope"
      (is (false? (api/await-ready! queue 0)))
      (is (nil? (api/claim! queue))))

    (doseq [envelope envelopes]
      (api/put! queue envelope))

    (testing "claims envelopes in insertion order"
      (is (true? (api/await-ready! queue 0)))
      (is (= envelopes (mapv (fn [_] (api/claim! queue)) envelopes)))
      (is (false? (api/await-ready! queue 0)))
      (is (nil? (api/claim! queue))))

    (testing "emits one put and claim event per envelope"
      (is (= [:producer/put :producer/put :producer/put
              :consumer/claim :consumer/claim :consumer/claim]
             (mapv :event @events)))
      (is (= (vec (concat envelopes envelopes))
             (mapv #(get-in % [:data :envelope]) @events))))))

(deftest acknowledge-test
  (let [events (atom [])
        errors (atom [])
        queue (fifo/fifo {:on-event #(swap! events conj %)
                          :on-error #(swap! errors conj %)})
        queued (envelope "1")
        unknown (envelope "unknown")]
    (api/put! queue queued)

    (testing "only claimed envelopes can be acknowledged"
      (is (nil? (api/ack! queue unknown)))
      (is (= :operation/failed (:event (first @errors))))
      (is (= :consumer/ack (:operation (first @errors))))
      (is (= unknown (:envelope (ex-data (:cause (first @errors)))))))

    (testing "acknowledging a claim makes its ID ready for removal"
      (is (= queued (api/claim! queue)))
      (is (nil? (api/ack! queue queued)))
      (is (true? (api/await-removed! queue 0)))
      (is (= [:producer/put :consumer/claim :consumer/ack]
             (mapv :event @events))))

    (testing "an envelope cannot be acknowledged twice"
      (api/ack! queue queued)
      (is (= 2 (count @errors))))))

(deftest message-id-lifecycle-test
  (let [queue (fifo/fifo)
        first-envelope (envelope "1")]
    (api/put! queue first-envelope)

    (testing "an ID remains reserved while queued, claimed, and acknowledged"
      (doseq [advance! [#(api/claim! queue)
                        #(api/ack! queue first-envelope)]]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"Message ID must be unique"
                              (api/put! queue (envelope "1"))))
        (advance!))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Message ID must be unique"
                            (api/put! queue (envelope "1")))))

    (testing "a successfully removed ID can be reused"
      (is (= #{"1"} (api/drain-removed! queue 10 (constantly true))))
      (is (nil? (api/put! queue (envelope "1"))))
      (is (= "1" (get-in (api/claim! queue) [:message :id]))))))

(deftest drain-removed-test
  (let [events (atom [])
        queue (fifo/fifo {:on-event #(swap! events conj %)})
        ids (set (map str (range 1 6)))
        envelopes (mapv envelope ids)
        batches (atom [])]
    (enqueue-and-ack! queue envelopes)

    (is (= ids (api/drain-removed! queue 2
                                   #(do (swap! batches conj %) true))))
    (is (= ids (set (mapcat identity @batches))))
    (is (every? #(<= 1 (count %) 2) @batches))
    (is (false? (api/await-removed! queue 0)))

    (testing "emits matching begin and end events for every accepted batch"
      (let [drain-events (filterv #(contains? #{:producer/begin-drain-removed
                                                :producer/end-drain-removed}
                                              (:event %))
                                  @events)]
        (is (= (* 2 (count @batches)) (count drain-events)))
        (is (= (mapv :data (take-nth 2 drain-events))
               (mapv :data (take-nth 2 (rest drain-events)))))))))

(deftest rejected-removal-is-restored-test
  (let [queue (fifo/fifo)
        ids #{"1" "2" "3"}
        envelopes (mapv envelope ids)]
    (enqueue-and-ack! queue envelopes)

    (testing "a falsy removal result retains the failed and unattempted IDs"
      (is (= #{} (api/drain-removed! queue 2 (constantly false))))
      (is (true? (api/await-removed! queue 0)))
      (is (= ids (api/drain-removed! queue 2 (constantly true))))
      (is (false? (api/await-removed! queue 0))))))

(deftest partial-removal-is-restored-test
  (let [queue (fifo/fifo)
        ids #{"1" "2" "3" "4"}
        envelopes (mapv envelope ids)
        calls (atom 0)]
    (enqueue-and-ack! queue envelopes)
    (let [drained (api/drain-removed! queue 2
                                      (fn [_]
                                        (= 1 (swap! calls inc))))]
      (is (= 2 (count drained)))
      (is (true? (api/await-removed! queue 0)))
      (let [remaining (api/drain-removed! queue 10 (constantly true))]
        (is (= ids (into drained remaining)))
        (is (empty? (set/intersection drained remaining)))))))

(deftest argument-validation-test
  (let [queue (fifo/fifo)
        assertions-enabled? (s/check-asserts?)]
    (s/check-asserts true)
    (try
      (is (thrown? clojure.lang.ExceptionInfo (api/put! queue {})))
      (is (thrown? clojure.lang.ExceptionInfo (api/ack! queue {})))
      (is (thrown? clojure.lang.ExceptionInfo (api/await-ready! queue -1)))
      (is (thrown? clojure.lang.ExceptionInfo (api/await-removed! queue -1)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (api/drain-removed! queue 0 (constantly true))))
      (is (thrown? clojure.lang.ExceptionInfo
                   (api/drain-removed! queue 1 :not-a-function)))
      (finally
        (s/check-asserts assertions-enabled?)))))