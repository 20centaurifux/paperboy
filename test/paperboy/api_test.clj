(ns paperboy.api-test
  (:require [clojure.test :refer [deftest is testing]]
            [paperboy.api :as api]))

(deftest envelope-creates-an-envelope
  (testing "path and message are retained"
    (let [message {:id "message-1" :payload "hello"}
          envelope (api/envelope "/notifications/email" message)]
      (is (= "/notifications/email" (:path envelope)))
      (is (identical? message (:message envelope))))))

(deftest envelope-validates-the-path
  (testing "valid paths"
    (doseq [path ["/a"
                  "/orders/created"
                  "/sensor-1/room_20"
                  (str "/" (apply str (repeat 20 "a")))]]
      (is (= path (:path (api/envelope path {:id "1" :payload "data"})))
          (str "expected valid path: " (pr-str path)))))

  (testing "invalid paths"
    (doseq [path [nil
                  ""
                  "orders"
                  "/"
                  "/orders/"
                  "/orders//created"
                  "/orders created"
                  "/orders.created"
                  (str "/" (apply str (repeat 21 "a")))]]
      (let [exception (try
                        (api/envelope path {:id "1" :payload "data"})
                        nil
                        (catch clojure.lang.ExceptionInfo exception
                          exception))]
        (is (some? exception)
            (str "expected invalid path: " (pr-str path)))
        (is (= "Invalid path" (ex-message exception)))
        (is (= path (:value (ex-data exception))))))))

(deftest envelope-validates-the-message
  (testing "invalid messages"
    (doseq [message [nil
                     {}
                     {:id "1"}
                     {:payload "data"}
                     {:id "" :payload "data"}
                     {:id "1" :payload ""}
                     {:id " " :payload "data"}
                     {:id "1" :payload " "}
                     {:id 1 :payload "data"}
                     {:id "1" :payload 42}]]
      (let [exception (try
                        (api/envelope "/orders" message)
                        nil
                        (catch clojure.lang.ExceptionInfo exception
                          exception))]
        (is (some? exception)
            (str "expected invalid message: " (pr-str message)))
        (is (= "Invalid message" (ex-message exception)))
        (is (= message (:value (ex-data exception))))))))