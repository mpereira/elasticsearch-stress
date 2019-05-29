(ns performance-benchmark-framework.statistics
  (:require [kixi.stats.core :as stats]
            [kixi.stats.distribution :as distribution :refer [quantile]]
            [performance-benchmark-framework.utils :refer [between? non-zero?]]))

(def ^{:dynamic true} *exponential-distribution-max-value-denominator-multiplier* 0.065)

(def ^{:dynamic true} *max-random-tries* 1000)

(defn statistics [xs]
  (let [distribution (transduce identity stats/histogram xs)]
    {:total (reduce + xs)
     :min (reduce stats/min xs)
     :max (reduce stats/max xs)
     :mean (transduce identity stats/mean xs)
     :sd (transduce identity stats/standard-deviation xs)
     :p50 (quantile distribution 0.50)
     :p75 (quantile distribution 0.75)
     :p90 (quantile distribution 0.90)
     :p95 (quantile distribution 0.95)
     :p99 (quantile distribution 0.99)}))

(defn clipped-normal-distribution [sample-size mean standard-deviation min max]
  {:pre [(<= min max)]}
  (let [distribution* (distribution/normal {:mu mean :sd standard-deviation})]
    (loop [sample-size sample-size
           full-sample []
           random-tries-remaining *max-random-tries*]
      (if (zero? random-tries-remaining)
        {:error [:unable-to-generate-clipped-distribution
                 {:sample-size sample-size
                  :mean mean
                  :standard-deviation standard-deviation
                  :min min
                  :max max}]}
        (let [sample (->> distribution*
                          (distribution/sample sample-size)
                          (filter (partial between? min max)))
              sample-count (count sample)
              new-full-sample (into full-sample sample)]
          (if (= sample-size sample-count)
            new-full-sample
            (recur (- sample-size sample-count)
                   new-full-sample
                   (dec random-tries-remaining))))))))

(defn rand-exponential
  [{:keys [rate max-value]}]
  (assert (not (and rate (non-zero? rate)
                    max-value (non-zero? max-value)))
          "Specify only one of 'rate' or 'max-value'")
  (let [rate
        (or rate
            (/ 1 (* *exponential-distribution-max-value-denominator-multiplier*
                    max-value)))]
    (first (distribution/exponential rate))))

(defn rand-exponential-int [& args]
  (Math/round (apply rand-exponential args)))

(defn standard-deviation-from-range [min* max*]
  (/ (- max* min*) 4.0))

(defn rand-normal [min* max* & [{:keys [skew sd-skew sd]
                                 :or {skew 0
                                      sd-skew 0}}]]
  (assert (not (and (non-zero? skew)
                    (non-zero? sd-skew)))
          "Specify only one of 'skew' or 'sd-skew'")
  (let [mean (/ (- max* min*) 2.0)
        standard-deviation (or sd (standard-deviation-from-range min* max*))
        actual-skew (if (zero? sd-skew)
                      skew
                      (* standard-deviation sd-skew))
        possibly-skewed-mean (+ mean actual-skew)
        sample-size 1
        {:keys [error] :as distribution-or-error}
        (clipped-normal-distribution
         sample-size possibly-skewed-mean standard-deviation min* max*)]
    (if error
      distribution-or-error
      (first distribution-or-error))))

(defn rand-normal-int [& args]
  (let [{:keys [error] :as rand-normal-or-error} (apply rand-normal args)]
    (if error
      rand-normal-or-error
      (Math/round rand-normal-or-error))))
