(ns paperboy.api-test
  (:require [clojure.test :refer [deftest is testing]]
            [paperboy.api :as api]
            [paperboy.test-utils :as test-utils]))

(deftest envelope-test
  (testing "creates an envelope"
    (let [message {:id "message-1"
                   :payload "hello"}
          envelope (api/envelope "/notifications/email" message)]
      (is (= "/notifications/email" (:path envelope)))
      (is (= message (:message envelope)))
      (is (record? envelope))))

  (testing "accepts path segments at the maximum length"
    (is (= "/abcdefghijklmnopqrst/12345678901234567890"
           (:path (api/envelope
                   "/abcdefghijklmnopqrst/12345678901234567890"
                   {:id "message-1" :payload "hello"}))))))

(deftest envelope-rejects-invalid-paths-test
  (let [message {:id "message-1" :payload "hello"}]
    (doseq [path [nil
                  ""
                  "notifications/email"
                  "/notifications/"
                  "/notifications//email"
                  "/notifications.email"
                  "/abcdefghijklmnopqrstu"]]
      (testing (pr-str path)
        (let [exception (test-utils/thrown-ex-info
                         #(api/envelope path message))]
          (is (some? exception))
          (is (= "Invalid path" (ex-message exception)))
          (is (= path (:value (ex-data exception))))
          (is (map? (:explain (ex-data exception)))))))))

(deftest envelope-rejects-invalid-messages-test
  (doseq [message [nil
                   {}
                   {:id "message-1"}
                   {:payload "hello"}
                   {:id "" :payload "hello"}
                   {:id "   " :payload "hello"}
                   {:id "message-1" :payload ""}
                   {:id "message-1" :payload "\t"}
                   {:id 1 :payload "hello"}
                   {:id "message-1" :payload 1}]]
    (testing (pr-str message)
      (let [exception (test-utils/thrown-ex-info
                       #(api/envelope "/notifications" message))]
        (is (some? exception))
        (is (= "Invalid message" (ex-message exception)))
        (is (= message (:value (ex-data exception))))
        (is (map? (:explain (ex-data exception))))))))