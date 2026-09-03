(ns paperboy.spooler.rocksdb-test
  (:require [clj-rocksdb :as rocks]
            [clojure.test :refer [deftest is testing]]
            [paperboy.api :as api]
            [paperboy.queue.fifo :as fifo]
            [paperboy.spooler.rocksdb :as rocksdb]
            [paperboy.test-utils :as test-utils])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temporary-directory []
  (str (Files/createTempDirectory
        "paperboy-rocksdb-test-"
        (make-array FileAttribute 0))))

(defmacro ^:private with-temporary-db [[directory] & body]
  `(let [~directory (temporary-directory)]
     (try
       ~@body
       (finally
         (rocks/destroy-db ~directory)))))

(defn- producer [put-fn]
  (reify api/Producer
    (put! [_ envelope]
      (put-fn envelope))
    (await-removed! [_ _]
      false)
    (drain-removed! [_ _ _]
      #{})))

(deftest rocksdb-test
  (with-temporary-db [directory]
    (with-open [spooler (rocksdb/rocksdb directory (producer identity))]
      (is (satisfies? api/Spooler spooler))
      (is (satisfies? api/Lifecycle spooler))
      (is (instance? java.io.Closeable spooler))))

  (testing "rejects invalid arguments"
    (doseq [[directory opts message]
            [["" {} "Invalid directory"]
             ["/tmp" {:on-event :not-a-function} "Invalid options"]
             ["/tmp" {:on-error :not-a-function} "Invalid options"]]]
      (let [exception (test-utils/thrown-ex-info
                       #(rocksdb/rocksdb directory (producer identity) opts))]
        (is (some? exception))
        (is (= message (ex-message exception)))))))

(deftest lifecycle-test
  (with-temporary-db [directory]
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
          spooler (rocksdb/rocksdb
                   directory
                   queue
                   {:on-event (fn [event]
                                (let [events' (swap! events conj event)]
                                  (when (= 4 (count events'))
                                    (deliver transitioned events'))))})]
      (try
        (api/start! spooler)

        (testing "await removed envelopes"
          (is (= @#'paperboy.spooler.rocksdb/cleanup-interval-ms
                 (deref awaited 2000 ::timeout))))

        (testing "purge removed envelopes"
          (is (= {:batch-size @#'paperboy.spooler.rocksdb/purge-batch-size
                  :removes-id? true}
                 (deref drained 2000 ::timeout))))
        (finally
          (api/stop! spooler)))

      (testing "emits lifecycle transitions in order"
        (is (= [{:event :lifecycle/transition
                 :sender :paperboy.spooler.rocksdb/rocksdb
                 :data {:from :stopped :to :starting}}
                {:event :lifecycle/transition
                 :sender :paperboy.spooler.rocksdb/rocksdb
                 :data {:from :starting :to :running}}
                {:event :lifecycle/transition
                 :sender :paperboy.spooler.rocksdb/rocksdb
                 :data {:from :running :to :stopping}}
                {:event :lifecycle/transition
                 :sender :paperboy.spooler.rocksdb/rocksdb
                 :data {:from :stopping :to :stopped}}]
               (deref transitioned 2000 ::timeout))))

      (testing "stop prevents further cleanup"
        (let [count-after-stop @drain-count]
          (Thread/sleep 500)
          (is (= count-after-stop @drain-count))))

      (.close ^java.io.Closeable spooler))))

(deftest restart-after-cleanup-failure-test
  (with-temporary-db [directory]
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
          spooler (rocksdb/rocksdb
                   directory
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
          (.close ^java.io.Closeable spooler)))

      (testing "restarts a failed cleanup worker"
        (is (= [[:stopped :starting]
                [:starting :running]
                [:running :failed]
                [:failed :starting]
                [:starting :running]
                [:running :stopping]
                [:stopping :stopped]]
               (mapv (juxt #(-> % :data :from) #(-> % :data :to))
                     (deref transitioned 2000 ::timeout))))))))

(deftest submit-and-restore-test
  (with-temporary-db [directory]
    (let [submitted (atom [])]
      (with-open [spooler (rocksdb/rocksdb
                           directory
                           (producer #(swap! submitted conj %)))]
        (testing "submits without starting the spooler"
          (let [first-envelope (api/submit! spooler
                                            "/notifications/email"
                                            "hello")
                second-envelope (api/submit! spooler
                                             "/notifications/sms"
                                             "world")]
            (is (= ["1" "2"]
                   (mapv #(get-in % [:message :id])
                         [first-envelope second-envelope])))
            (is (= [first-envelope second-envelope] @submitted)))))

      (let [restored (atom [])]
        (with-open [spooler (rocksdb/rocksdb
                             directory
                             (producer #(swap! restored conj %)))]
          (testing "restores persisted envelopes during construction"
            (is (= [{:path "/notifications/email"
                     :message {:id "1" :payload "hello"}}
                    {:path "/notifications/sms"
                     :message {:id "2" :payload "world"}}]
                   (mapv #(into {} %) @restored))))

          (testing "continues numbering after the greatest restored ID"
            (is (= "3"
                   (get-in (api/submit! spooler
                                        "/notifications/push"
                                        "again")
                           [:message :id])))))))))

(deftest restore-failure-test
  (with-temporary-db [directory]
    (with-open [spooler (rocksdb/rocksdb directory (producer identity))]
      (api/submit! spooler "/notifications/email" "hello")
      (api/submit! spooler "/notifications/sms" "world"))

    (let [failure (ex-info "queue unavailable" {:queue :test})
          restored-before-failure (atom [])
          put-count (atom 0)]
      (testing "rethrows a queue failure during restoration"
        (let [thrown (test-utils/thrown-ex-info
                      #(rocksdb/rocksdb
                        directory
                        (producer
                         (fn [envelope]
                           (if (= 2 (swap! put-count inc))
                             (throw failure)
                             (swap! restored-before-failure conj envelope))))))]
          (is (identical? failure thrown))))

      (testing "does not roll back envelopes restored before the failure"
        (is (= ["1"]
               (mapv #(get-in % [:message :id])
                     @restored-before-failure)))))

    (testing "retains all persisted messages"
      (let [restored (atom [])]
        (with-open [_ (rocksdb/rocksdb
                       directory
                       (producer #(swap! restored conj %)))]
          (is (= [{:path "/notifications/email"
                   :message {:id "1" :payload "hello"}}
                  {:path "/notifications/sms"
                   :message {:id "2" :payload "world"}}]
                 (mapv #(into {} %) @restored))))))))

(deftest submit-validation-test
  (with-temporary-db [directory]
    (let [put-count (atom 0)
          events (atom [])]
      (with-open [spooler (rocksdb/rocksdb
                           directory
                           (producer (fn [_] (swap! put-count inc)))
                           {:on-event #(swap! events conj %)})]
        (doseq [[path payload] [["notifications" "hello"]
                                ["/notifications" " "]]]
          (is (thrown? clojure.lang.ExceptionInfo
                       (api/submit! spooler path payload))))
        (is (zero? @put-count))
        (is (empty? @events))))))

(deftest submit-events-test
  (with-temporary-db [directory]
    (let [events (atom [])]
      (with-open [spooler (rocksdb/rocksdb
                           directory
                           (producer identity)
                           {:on-event #(swap! events conj %)})]
        (api/submit! spooler "/notifications/email" "hello")
        (api/submit! spooler "/notifications/sms" "world")
        (is (= [{:event :spooler/begin-submit
                 :sender :paperboy.spooler.rocksdb/rocksdb
                 :data {:path "/notifications/email" :payload "hello"}}
                {:event :spooler/end-submit
                 :sender :paperboy.spooler.rocksdb/rocksdb
                 :data {:path "/notifications/email" :payload "hello"}}
                {:event :spooler/begin-submit
                 :sender :paperboy.spooler.rocksdb/rocksdb
                 :data {:path "/notifications/sms" :payload "world"}}
                {:event :spooler/end-submit
                 :sender :paperboy.spooler.rocksdb/rocksdb
                 :data {:path "/notifications/sms" :payload "world"}}]
               @events))))))

(deftest submit-queue-failure-test
  (with-temporary-db [directory]
    (let [failure (ex-info "queue unavailable" {:queue :test})
          events (atom [])
          errors (atom [])]
      (with-open [spooler (rocksdb/rocksdb
                           directory
                           (producer (fn [_] (throw failure)))
                           {:on-event #(swap! events conj %)
                            :on-error #(swap! errors conj %)})]
        (testing "rethrows the queue exception"
          (is (identical? failure
                          (test-utils/thrown-ex-info
                           #(api/submit! spooler
                                         "/notifications"
                                         "hello")))))

        (testing "emits begin and failure events, but no end event"
          (is (= [{:event :spooler/begin-submit
                   :sender :paperboy.spooler.rocksdb/rocksdb
                   :data {:path "/notifications" :payload "hello"}}]
                 @events))
          (is (= [{:event :operation/failed
                   :sender :paperboy.spooler.rocksdb/rocksdb
                   :operation :spooler/submit
                   :cause failure}]
                 @errors))))

      (let [restored (atom [])]
        (with-open [_ (rocksdb/rocksdb
                       directory
                       (producer #(swap! restored conj %)))]
          (testing "keeps the message persisted for restoration"
            (is (= [{:path "/notifications"
                     :message {:id "1" :payload "hello"}}]
                   (mapv #(into {} %) @restored)))))))))

(deftest failed-submit-consumes-id-test
  (with-temporary-db [directory]
    (let [failure (ex-info "queue unavailable" {:queue :test})
          attempts (atom [])
          spooler (rocksdb/rocksdb
                   directory
                   (producer
                    (fn [envelope]
                      (let [attempt (count (swap! attempts conj envelope))]
                        (when (= 1 attempt)
                          (throw failure))))))]
      (try
        (is (identical? failure
                        (test-utils/thrown-ex-info
                         #(api/submit! spooler
                                       "/notifications"
                                       "first"))))

        (let [envelope (api/submit! spooler
                                    "/notifications"
                                    "second")]
          (testing "a failed queue put consumes its persisted message ID"
            (is (= "2" (get-in envelope [:message :id])))
            (is (= ["1" "2"]
                   (mapv #(get-in % [:message :id]) @attempts)))))
        (finally
          (.close ^java.io.Closeable spooler))))))

(deftest cleanup-failure-retains-message-test
  (with-temporary-db [directory]
    (let [failure (ex-info "RocksDB delete failed" {:db :test})
          cleanup-failed (promise)
          queue (fifo/fifo
                 {:on-error (fn [event]
                              (when (= :producer/drain-removed
                                       (:operation event))
                                (deliver cleanup-failed event)))})]
      (with-open [spooler (rocksdb/rocksdb directory queue)]
        (let [envelope (api/submit! spooler "/notifications" "hello")]
          (is (= envelope (api/claim! queue)))
          (api/ack! queue envelope))

        (with-redefs [rocks/delete (fn [& _] (throw failure))]
          (try
            (api/start! spooler)
            (let [error (deref cleanup-failed 2000 ::timeout)]
              (is (= :operation/failed (:event error)))
              (is (= :producer/drain-removed (:operation error)))
              (is (identical? failure (:cause error))))
            (finally
              (api/stop! spooler)))))

      (let [restored (atom [])]
        (with-open [_ (rocksdb/rocksdb
                       directory
                       (producer #(swap! restored conj %)))]
          (is (= [{:path "/notifications"
                   :message {:id "1" :payload "hello"}}]
                 (mapv #(into {} %) @restored))))))))

(deftest cleanup-lifecycle-test
  (with-temporary-db [directory]
    (let [cleanup-finished (promise)
          queue (fifo/fifo
                 {:on-event (fn [event]
                              (when (= :producer/end-drain-removed
                                       (:event event))
                                (deliver cleanup-finished true)))})]
      (with-open [spooler (rocksdb/rocksdb directory queue)]
        (testing "started cleanup removes acknowledged messages from RocksDB"
          (api/start! spooler)
          (let [envelope (api/submit! spooler "/notifications" "hello")]
            (is (= envelope (api/claim! queue)))
            (api/ack! queue envelope)
            (is (= true (deref cleanup-finished 2000 ::timeout)))))

        (testing "stopped cleanup does not close the spooler"
          (api/stop! spooler)
          (is (= "2"
                 (get-in (api/submit! spooler
                                      "/notifications"
                                      "after stop")
                         [:message :id]))))

        (testing "the cleanup worker can be restarted"
          (api/start! spooler)
          (api/stop! spooler)))

      (testing "only the message submitted after cleanup stopped is restored"
        (let [restored (atom [])]
          (with-open [_ (rocksdb/rocksdb
                         directory
                         (producer #(swap! restored conj %)))]
            (is (= ["2"]
                   (mapv #(get-in % [:message :id]) @restored)))))))))

(deftest close-test
  (with-temporary-db [directory]
    (let [spooler (rocksdb/rocksdb directory (producer identity))]
      (.close ^java.io.Closeable spooler)

      (testing "close is idempotent"
        (is (nil? (.close ^java.io.Closeable spooler))))

      (testing "a closed spooler cannot be started or submitted to"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"RocksDB spooler is closed"
                              (api/start! spooler)))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"RocksDB spooler is closed"
                              (api/submit! spooler
                                           "/notifications"
                                           "hello")))))))