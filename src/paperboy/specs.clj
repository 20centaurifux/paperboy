(ns paperboy.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]))

(s/def ::non-blank-string
  (s/and string?
         (complement str/blank?)))

;;; Envelope

(s/def ::path
  (s/and string?
         #(boolean
           (re-matches #"^/[0-9a-zA-Z_-]{1,20}(?:/[0-9a-zA-Z_-]{1,20})*$" %))))

(s/def ::id ::non-blank-string)

(s/def ::payload ::non-blank-string)

(s/def ::message (s/keys :req-un [::id ::payload]))

(s/def ::envelope (s/keys :req-un [::path ::message]))

;; Options

(s/def ::on-event fn?)

(s/def ::on-error fn?)

(s/def ::event-opts (s/keys :opt-un [::on-event ::on-error]))