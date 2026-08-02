(ns paperboy.utils
  (:require [clojure.spec.alpha :as s]))

(defn validate!
  [spec message value]
  (when-not (s/valid? spec value)
    (throw (ex-info message
                    {:value value
                     :explain (s/explain-data spec value)}))))