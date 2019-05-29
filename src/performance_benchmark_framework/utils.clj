(ns performance-benchmark-framework.utils
  (:require [clojure.pprint :refer [pprint]]))

(def non-zero? (complement zero?))

(defn between? [min max n]
  (<= min n max))

(defn pprint-str [x] (with-out-str (pprint x)))
