(ns performance-benchmark-framework.core
  (:gen-class)
  (:require [cheshire.core :as json]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as string]
            [com.climate.claypoole :as cp]
            [oz.core :as oz]
            [kixi.stats.core :as stats]
            [kixi.stats.distribution
             :refer [quantile]
             :as distribution]
            [qbits.spandex :as s]
            [taoensso.timbre :refer [info]])
  (:import (java.util Random)))

(def ^{:dynamic true} *exponential-distribution-max-value-denominator-multiplier* 0.065)

(def ^{:dynamic true} *token-size-jitter* 0.2)

(def ^{:dynamic true} *max-random-tries* 1000)

(defn pprint-str [x] (with-out-str (pprint x)))

(defn between? [min max n]
  (<= min n max))

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

(defmacro runtime [body]
  `(let [start# (. System (nanoTime))
         result# ~body]
     {:runtime-ms (/ (- (. System (nanoTime))
                        start#)
                     1000000.0)
      :value result#}))

(defn create-document
  ([client index-name document]
   (pprint document)
   (-> (s/request client {:url [index-name :_doc]
                          :method :post
                          :body document})))
  ([client index-name document-id document]
   (-> (s/request client
                  {:url [index-name :_create document-id]
                   :method :put
                   :body document}))))

(defn- make-bulk-operation
  [make-action-and-metadata]
  (fn bulk-operation-fn [client index-name documents]
    (let [bulk-data (->> documents
                         (mapcat (fn [document]
                                   (map json/generate-string
                                        (make-action-and-metadata document))))
                         (string/join "\n")
                         (#(str % "\n"))
                         (s/raw))]
      (s/request client
                 {:url [index-name :_bulk]
                  :method :post
                  :headers {"Content-Type" "application/x-ndjson"}
                  :body bulk-data}))))

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

(defn create-documents [client index-name documents]
  ((make-bulk-operation (fn [document] [{:index {}} document]))
   client index-name documents))

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

(def non-zero? (complement zero?))

(defn rand-exponential [max-value]
  (let [rate (/ 1 (* *exponential-distribution-max-value-denominator-multiplier*
                     max-value))]
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
                  (clipped-normal-distribution 1 mean standard-deviation min* max*)]
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
        {:keys [error] :as number-of-kvs-or-error} (+ minimum-number-of-kvs
                                                      (rand-exponential-int
                                                       maximum-number-of-kvs))]
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
            sd-skew -3
            sd (/ (standard-deviation-from-range minimum-size-for-ks
                                                 maximum-size-for-ks)
                  (Math/abs sd-skew))
            {:keys [error] :as size-for-ks-or-error} (rand-normal-int
                                                      minimum-size-for-ks
                                                      maximum-size-for-ks
                                                      {:sd sd
                                                       :sd-skew sd-skew})]
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
                                         (assoc properties field {:type "keyword"}))
                                       {}
                                       fields)}
         :size-remaining-for-values size-remaining-for-values}))))

(defn generate-document [{:keys [properties] :as mapping} available-size]
  (let [generator-outputs (generate-tokens generate-value (count properties) available-size)]
    (into {} (map vector (keys properties) generator-outputs))))

(defn generate-document-batches
  [number-of-documents bulk-size document-size mapping size-for-values]
  (->> (repeatedly number-of-documents #(generate-document mapping size-for-values))
       (partition-all bulk-size)))

(defn process-bulk-batch [client index-name batch]
  (let [{:keys [runtime-ms]
         {:keys [hosts]
          {:keys [took errors items]} :body
          bulk-status :status} :value}
        (runtime (create-documents client index-name batch))
        outcomes (reduce (fn [memo
                              {{:keys [status result]
                                {:keys [total
                                        successful
                                        failed]} :_shards} :index
                               :as item}]
                           (merge-with conj
                                       memo
                                       {:status status
                                        :total total
                                        :successful successful
                                        :failed failed
                                        :result result}))
                         {:status []
                          :total []
                          :successful []
                          :failed []
                          :result []}
                         items)]
    (assoc outcomes
           :runtime-ms [runtime-ms]
           :took [took]
           :bulk-status [bulk-status])))

(defn process-document [client index-name document]
  (let [{:keys [runtime-ms]
         {:keys [status]
          {{:keys [total
                   successful
                   failed]} :_shards} :body} :value}
        (runtime (create-document client index-name document))]
    {:runtime-ms runtime-ms
     :status status
     :total total
     :successful successful
     :failed failed}))

(defn run [{:keys [bulk
                   bulk-size
                   document-size
                   documents
                   hosts
                   index-name
                   threads]
            :or {bulk false
                 bulk-size 50
                 document-size 500 ;; size of JSON object in bytes.
                 hosts ["http://localhost:9200"]
                 index-name "elasticsearch-stress"
                 threads 1}}]
  (let [{:keys [mapping size-remaining-for-values]} (generate-mapping document-size)]
    (info "")
    (info "Documents:" documents)
    (info "Document size:" document-size)
    (info "Index name:" index-name)
    (info "Bulk size:" bulk-size)
    (info "Bulk:" bulk)
    (info "Mapping:" (pprint-str mapping))
    (info "")
    (info "Starting load")
    (let [client (s/client {:hosts ["http://localhost:9200"
                                    "http://localhost:9201"]})
          pool (cp/threadpool threads :name "document-indexer")
          {total-runtime-ms :runtime-ms
           outcomes :value}
          (runtime
           (if bulk
             (->> (generate-document-batches documents
                                             bulk-size
                                             document-size
                                             mapping
                                             size-remaining-for-values)
                  (cp/pmap pool (partial process-bulk-batch client index-name))
                  (reduce (partial merge-with into)
                          {:runtime-ms []
                           :took []
                           :bulk-status []
                           :status []
                           :total []
                           :successful []
                           :failed []
                           :result []}))
             (let [documents* (repeatedly documents
                                          #(generate-document
                                            mapping size-remaining-for-values))]
               (->> documents*
                    (cp/pmap pool (partial process-document client index-name))
                    (reduce (partial merge-with conj)
                            {:runtime-ms []
                             :status []
                             :total []
                             :successful []
                             :failed []})))))
          report (-> outcomes
                     (update :runtime-ms statistics)
                     (update :bulk-status frequencies)
                     (update :status frequencies)
                     (update :result frequencies)
                     (update :took statistics)
                     (update :total (partial reduce +))
                     (update :successful (partial reduce +))
                     (update :failed (partial reduce +)))]
      (info (with-out-str (pprint report)))
      (info "Total Elasticsearch 'took' runtime:"
            (format "%dms" (get-in report [:took :total])))
      (info "Total observed runtime:" (format "%.2fms" total-runtime-ms))
      (cp/shutdown pool))
    (info "Ended load")))

(comment
  (let [{:keys [fields size-remaining-for-values]} (generate-fields 500)]
    (pprint (take 5 (repeatedly #(generate-document fields size-remaining-for-values)))))

  (pprint (s/request (s/client {:hosts ["http://localhost:9200"
                                        "http://localhost:9201"]})
                     {:url [:elasticsearch-stress]
                      :method :get}))
  (pprint (s/request (s/client {:hosts ["http://localhost:9200"
                                        "http://localhost:9201"]})
                     {:url [:_search]
                      :method :get
                      :body {:query {:match_all {}}}}))
  (pprint (create-document (s/client {:hosts ["http://localhost:9200"
                                              "http://localhost:9201"]})
                           :elasticsearch-stress
                           {:foo "foo"
                            :bar "bar"}))
  (pprint (s/request (s/client {:hosts ["http://localhost:9200"
                                        "http://localhost:9201"]})
                     {:url [:elasticsearch-stress]
                      :method :delete}))

  (oz/export! {:data {:values (->> (repeatedly
                                    100000
                                    #(rand-normal-int
                                      35
                                      754
                                      {:sd (/ (standard-deviation-from-range 35 754)
                                              2)
                                       :sd-skew -2}))

                                   (map hash-map (repeat :x)))}
               :mark "bar"
               :encoding {:x {:bin {:binned true
                                    :step 5}
                              :field "x"}
                          :y {:aggregate "count"
                              :type "quantitative"}}}
              "normal_distribution.html")

  (oz/export! {:data {:values (->> (repeatedly
                                    100000
                                    #(rand-exponential 1000))
                                   (map hash-map (repeat :x)))}
               :mark "bar"
               :encoding {:x {:bin {:binned true
                                    :step 50}
                              :field "x"}
                          :y {:aggregate "count"
                              :type "quantitative"}}}
              "exponential_distribution.html"))

(defn -main
  [& args]
  (run {:bulk true
        :bulk-size 500
        :document-size 1000
        :documents 5000
        :hosts ["http://localhost:9200" "http://localhost:9201"]
        :index-name "elasticsearch-stress"
        :threads 4}))
