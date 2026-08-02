(ns paperboy.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]))

(s/def ::path
  (s/and string?
         #(boolean
           (re-matches #"^/[0-9a-zA-Z_-]{1,20}(?:/[0-9a-zA-Z_-]{1,20})*$" %))))

(s/def ::non-blank-string
  (s/and string?
         (complement str/blank?)))


(s/def ::id ::non-blank-string)

(s/def ::payload ::non-blank-string)

(s/def ::message (s/keys :req-un [::id ::payload]))