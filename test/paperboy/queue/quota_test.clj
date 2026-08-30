(ns paperboy.queue.quota-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest is testing]]
            [paperboy.api :as api]
            [paperboy.queue.quota :as quota]
            [paperboy.test-utils :as test-utils]))

(defn- envelope
  [path id]
  (api/envelope path {:id id :payload (str "payload-" id)}))

(deftest quota-test
  (let [queue (quota/quota {:size 10})]
    (is (satisfies? api/Producer queue))
    (is (satisfies? api/Consumer queue)))

  (testing "rejects invalid quotas"
    (doseq [quotas [{} {:size 0} {:size 1 :routes {"/x/*" {:size 0}}}]]
      (let [exception (test-utils/thrown-ex-info #(quota/quota quotas))]
        (is (some? exception))
        (is (= "Invalid quotas" (ex-message exception)))
        (is (= quotas (:value (ex-data exception)))))))

  (testing "rejects invalid event callbacks"
    (doseq [opts [{:on-event :not-a-function}
                  {:on-error :not-a-function}]]
      (let [exception (test-utils/thrown-ex-info
                       #(quota/quota {:size 10} opts))]
        (is (some? exception))
        (is (= "Invalid options" (ex-message exception)))
        (is (= opts (:value (ex-data exception))))))))

(deftest route-quota-test
  (let [queue (quota/quota {:size 2
                            :routes {"/tenant/a/*" {:size 1}}})
        first-a (envelope "/tenant/a/first" "a-1")
        second-a (envelope "/tenant/a/second" "a-2")
        first-default (envelope "/tenant/b/first" "b-1")
        second-default (envelope "/tenant/b/second" "b-2")
        third-default (envelope "/tenant/b/third" "b-3")]
    (api/put! queue first-a)
    (api/put! queue second-a)
    (api/put! queue first-default)
    (api/put! queue second-default)
    (api/put! queue third-default)

    (testing "each concrete match keeps its own quota-sized tail"
      (is (true? (api/await-ready! queue 0)))
      (is (= [first-a second-a second-default third-default]
             [(api/claim! queue)
              (api/claim! queue)
              (api/claim! queue)
              (api/claim! queue)]))
      (is (false? (api/await-ready! queue 0)))
      (is (nil? (api/claim! queue))))

    (testing "evicted message IDs are ready for removal"
      (is (true? (api/await-removed! queue 0)))
      (is (= #{"b-1"}
             (api/drain-removed! queue 10 (constantly true))))
      (is (false? (api/await-removed! queue 0))))))

(deftest acknowledge-test
  (let [events (atom [])
        errors (atom [])
        queue (quota/quota {:size 10}
                           {:on-event #(swap! events conj %)
                            :on-error #(swap! errors conj %)})
        queued (envelope "/notifications" "1")
        unknown (envelope "/notifications" "unknown")]
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
             (mapv :event @events))))))

(deftest message-id-lifecycle-test
  (let [queue (quota/quota {:size 1})
        first-envelope (envelope "/notifications" "1")
        second-envelope (envelope "/notifications" "2")]
    (api/put! queue first-envelope)
    (api/put! queue second-envelope)

    (testing "evicted IDs remain reserved until removal is drained"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Message ID must be unique"
                            (api/put! queue first-envelope)))
      (is (= #{"1"} (api/drain-removed! queue 10 (constantly true))))
      (is (nil? (api/put! queue first-envelope))))))

(deftest argument-validation-test
  (let [queue (quota/quota {:size 10})
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