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

(deftest lifecycle-test
  (let [awaited (promise)
        drained (promise)
        transitioned (promise)
        drain-count (atom 0)
        events (atom [])
        queue (reify api/Producer
                (put! [_ _])
                (await-removed! [_ timeout-ms]
                  (deliver awaited timeout-ms)
                  true)
                (drain-removed! [_ batch-size remove-fn]
                  (let [call {:batch-size batch-size
                              :removes-id? (remove-fn #{})}]
                    (swap! drain-count inc)
                    (deliver drained call)
                    #{})))
        spooler (passthrough/passthrough
                 queue
                 {:on-event (fn [event]
                              (let [events' (swap! events conj event)]
                                (when (= 4 (count events'))
                                  (deliver transitioned events'))))})]
    (try
      (api/start! spooler)

      (testing "await removed envelopes"
        (is (= @#'paperboy.spooler.passthrough/cleanup-interval-ms
               (deref awaited 2000 ::timeout))))

      (testing "purge removed envelopes"
        (is (= {:batch-size @#'paperboy.spooler.passthrough/purge-batch-size
                :removes-id? true}
               (deref drained 2000 ::timeout))))
      (finally
        (api/stop! spooler)))

    (testing "emits lifecycle transitions in order"
      (is (= [{:event :lifecycle/transition
               :sender :paperboy.spooler.passthrough/passthrough
               :data {:from :stopped :to :starting}}
              {:event :lifecycle/transition
               :sender :paperboy.spooler.passthrough/passthrough
               :data {:from :starting :to :running}}
              {:event :lifecycle/transition
               :sender :paperboy.spooler.passthrough/passthrough
               :data {:from :running :to :stopping}}
              {:event :lifecycle/transition
               :sender :paperboy.spooler.passthrough/passthrough
               :data {:from :stopping :to :stopped}}]
             (deref transitioned 2000 ::timeout))))

    (testing "stop prevents further cleanup"
      (let [count-after-stop @drain-count]
        (Thread/sleep 500)
        (is (= count-after-stop @drain-count))))))

(deftest restart-after-cleanup-failure-test
  (let [failure (ex-info "cleanup failed" {:queue :test})
        failed (promise)
        restarted (promise)
        transitioned (promise)
        drain-count (atom 0)
        events (atom [])
        queue (reify api/Producer
                (put! [_ _])
                (await-removed! [_ _]
                  true)
                (drain-removed! [_ _ _]
                  (if (= 1 (swap! drain-count inc))
                    (throw failure)
                    (do
                      (deliver restarted true)
                      #{}))))
        spooler (passthrough/passthrough
                 queue
                 {:on-event (fn [event]
                              (let [events' (swap! events conj event)]
                                (when (= :failed (-> event :data :to))
                                  (deliver failed event))
                                (when (= 7 (count events'))
                                  (deliver transitioned events'))))})]
    (try
      (api/start! spooler)
      (is (identical? failure
                      (-> (deref failed 2000 ::timeout) :data :cause)))

      (api/start! spooler)
      (is (= true (deref restarted 2000 ::timeout)))
      (finally
        (api/stop! spooler)))

    (testing "restarts a failed cleanup worker"
      (is (= [[:stopped :starting]
              [:starting :running]
              [:running :failed]
              [:failed :starting]
              [:starting :running]
              [:running :stopping]
              [:stopping :stopped]]
             (mapv (juxt #(-> % :data :from) #(-> % :data :to))
                   (deref transitioned 2000 ::timeout)))))))

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
               :sender :paperboy.spooler.passthrough/passthrough
               :data {:path "/notifications/email" :payload "hello"}}
              {:event :spooler/end-submit
               :sender :paperboy.spooler.passthrough/passthrough
               :data {:path "/notifications/email" :payload "hello"}}
              {:event :spooler/begin-submit
               :sender :paperboy.spooler.passthrough/passthrough
               :data {:path "/notifications/sms" :payload "world"}}
              {:event :spooler/end-submit
               :sender :paperboy.spooler.passthrough/passthrough
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
      (is (= 1 (count @events)))
      (is (= {:event :operation/failed
              :sender :paperboy.spooler.passthrough/passthrough
              :operation :spooler/submit
              :cause failure}
             (first @errors))))))

(deftest submit-after-queue-failure-test
  (let [failure (ex-info "queue unavailable" {:queue :test})
        attempts (atom [])
        spooler (passthrough/passthrough
                 (producer (fn [envelope]
                             (let [attempt (count (swap! attempts conj envelope))]
                               (when (= 1 attempt)
                                 (throw failure))))))]
    (is (identical? failure
                    (test-utils/thrown-ex-info
                     #(api/submit! spooler "/notifications" "first"))))

    (let [envelope (api/submit! spooler "/notifications" "second")]
      (testing "a failed put consumes its message ID"
        (is (= "2" (-> envelope :message :id)))
        (is (= ["1" "2"]
               (mapv #(-> % :message :id) @attempts)))))))