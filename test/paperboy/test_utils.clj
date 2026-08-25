(ns paperboy.test-utils)

(defn thrown-ex-info
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo exception
      exception)))