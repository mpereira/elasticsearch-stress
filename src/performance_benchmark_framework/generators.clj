(ns performance-benchmark-framework.generators
  (:require [performance-benchmark-framework.statistics
             :refer [*max-random-tries*
                     clipped-normal-distribution
                     rand-exponential-int]]))

(def ^{:dynamic true} *token-size-jitter* 0.2)

(def digit (range 0 10))

(def lowercase-alpha (map char (range (int \a) (inc (int \z)))))

(def uppercase-alpha (map char (range (int \A) (inc (int \Z)))))

(def symbol* [\- \_ \space \,])

(def lowercase-alpha-numeric
  (concat lowercase-alpha digit))

(def uppercase-alpha-numeric
  (concat uppercase-alpha digit))

(def upper-and-lowercase-alpha-numeric
  (concat uppercase-alpha lowercase-alpha-numeric))

(defn generate-field [size]
  {:pre [(or (zero? size) (pos? size))]}
  (apply str
         (when (pos? size) (rand-nth lowercase-alpha))
         (take (dec size)
               (repeatedly #(rand-nth lowercase-alpha-numeric)))))

(defn generate-value [size]
  {:pre [(or (zero? size) (pos? size))]}
  (->> #(rand-nth (concat lowercase-alpha-numeric symbol*))
       (repeatedly)
       (take size)
       (apply str)))

(defn uniform-mean-statistics
  "Breaks down `available-size` in `tokens-count` parts.

  Generates statistics so that:
  1. 'mean' is 'available-size / tokens-count'
  2. 'standard-deviation' is 'mean * jitter'"
  [available-size tokens-count minimum-token-size]
  (let [minimum-required-size (* minimum-token-size
                                 tokens-count)
        minimum-required-size-for-rest (- minimum-required-size
                                          minimum-token-size)
        min* minimum-token-size
        max* (max min*
                  (- available-size
                     minimum-required-size-for-rest))
        range* (- max* min*)
        mean (if (zero? range*)
               min*
               (/ available-size tokens-count))
        standard-deviation (if (zero? range*)
                             range*
                             (Math/abs (* *token-size-jitter* mean)))]
    {:min min*
     :max max*
     :mean mean
     :standard-deviation standard-deviation}))

(defn generate-token [available-size tokens-to-go minimum-token-size generator]
  (let [{:keys [error] :as token-size-or-error}
        (if (zero? available-size)
          0
          (if (= 1 tokens-to-go)
            available-size
            (let [{:keys [mean standard-deviation] min* :min max* :max}
                  (uniform-mean-statistics available-size
                                           tokens-to-go
                                           minimum-token-size)
                  {:keys [error] :as distribution-or-error}
                  (clipped-normal-distribution
                   1 mean standard-deviation min* max*)]
              (if error
                distribution-or-error
                (Math/round (first distribution-or-error))))))]
    (if error
      token-size-or-error
      (generator token-size-or-error))))

(defn generate-tokens
  [generator number-of-tokens available-size & [{:keys [unique?]
                                                 :or {unique? false}}]]
  (let [args {:generator generator
              :number-of-tokens number-of-tokens
              :available-size available-size
              :unique? unique?}]
    (loop [available-size available-size
           tokens (if unique? #{} [])
           unique-attempts-remaining *max-random-tries*]
      (let [minimum-token-size 1
            tokens-to-go (- number-of-tokens (count tokens))]
        (if (and unique? (zero? unique-attempts-remaining))
          {:error [:unable-to-generate-tokens :ran-out-of-attempts args]}
          (if (zero? tokens-to-go)
            tokens
            (letfn [(make-token []
                      (generate-token available-size
                                      tokens-to-go
                                      minimum-token-size
                                      generator))]
              (if unique?
                (if (zero? available-size)
                  (if (zero? tokens-to-go)
                    tokens
                    {:error [:unable-to-generate-tokens :impossible args]})
                  (let [{:keys [error] :as token-or-error} (make-token)]
                    (if error
                      token-or-error
                      (if (contains? tokens token-or-error)
                        (recur available-size
                               tokens
                               (dec unique-attempts-remaining))
                        (recur (- available-size (count token-or-error))
                               (conj tokens token-or-error)
                               unique-attempts-remaining)))))
                (let [{:keys [error] :as token-or-error} (make-token)]
                  (if error
                    token-or-error
                    (recur (- available-size (count token-or-error))
                           (conj tokens token-or-error)
                           unique-attempts-remaining)))))))))))

(defn generate-fields [document-size]
  (let [document-overhead 2      ;; {}
        minimum-token-size 1     ;; a
        kv-overhead 5            ;; "":""
        additional-kv-overhead 1 ;; ,
        minimum-kv-size 7        ;; "a":"b"
        minimum-k-size minimum-token-size
        minimum-v-size minimum-token-size
        minimum-document-size (+ document-overhead minimum-kv-size)
        minimum-number-of-kvs 1
        maximum-number-of-kvs
        (loop [available-size (- document-size document-overhead)
               number-of-kvs 0]
          (let [possible-additional-kv-overhead (if (pos? number-of-kvs)
                                                  additional-kv-overhead
                                                  0)
                available-size-with-another-kv
                (- available-size
                   (+ minimum-kv-size
                      possible-additional-kv-overhead))]
            (if (> 0 available-size-with-another-kv)
              number-of-kvs
              (recur available-size-with-another-kv
                     (inc number-of-kvs)))))
        {:keys [error] :as number-of-kvs-or-error} (+
                                                    minimum-number-of-kvs
                                                    (rand-exponential-int
                                                     {:max-value
                                                      maximum-number-of-kvs}))]
    (if error
      number-of-kvs-or-error
      (let [number-of-kvs number-of-kvs-or-error
            number-of-ks number-of-kvs
            number-of-vs number-of-kvs
            total-overhead (+ document-overhead
                              (* number-of-kvs kv-overhead)
                              (* (- number-of-kvs 1) additional-kv-overhead))
            current-available-size (- document-size total-overhead)
            minimum-size-for-ks (* minimum-token-size number-of-ks)
            minimum-size-for-vs (* minimum-token-size number-of-vs)
            maximum-size-for-ks (- current-available-size minimum-size-for-vs)
            {:keys [error] :as size-for-ks-or-error} (+
                                                      (max minimum-size-for-ks
                                                           number-of-kvs)
                                                      (rand-exponential-int
                                                       {:max-value
                                                        maximum-size-for-ks}))]
        (if error
          size-for-ks-or-error
          (let [size-for-ks size-for-ks-or-error
                size-for-vs (- current-available-size size-for-ks)
                {:keys [error] :as tokens-or-error} (generate-tokens
                                                     generate-field
                                                     number-of-kvs
                                                     size-for-ks
                                                     {:unique? true})]
            (if error
              tokens-or-error
              {:fields tokens-or-error
               :size-remaining-for-values size-for-vs})))))))

(defn generate-mapping [document-size]
  (let [{:keys [error] :as result-or-error} (generate-fields document-size)]
    (if error
      result-or-error
      (let [{:keys [fields size-remaining-for-values]} result-or-error]
        {:mapping {:properties (reduce (fn [properties field]
                                         (assoc properties
                                                field {:type "keyword"}))
                                       {}
                                       fields)}
         :size-remaining-for-values size-remaining-for-values}))))

(defn generate-document [{:keys [properties] :as mapping} available-size]
  (let [generator-outputs (generate-tokens generate-value
                                           (count properties)
                                           available-size)]
    (into {} (map vector (keys properties) generator-outputs))))

(defn generate-document-batches
  [number-of-documents bulk-size document-size mapping size-for-values]
  (->> #(generate-document mapping size-for-values)
       (repeatedly number-of-documents)
       (partition-all bulk-size)))
