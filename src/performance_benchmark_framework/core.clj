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

(defn between? [min max n]
  (<= min n max))

(defn clipped-normal-distribution [sample-size mean standard-deviation min max]
  (let [distribution* (distribution/normal {:mu mean :sd standard-deviation})]
    (loop [sample-size sample-size
           full-sample []]
      (let [sample (->> distribution*
                        (distribution/sample sample-size)
                        (filter (partial between? min max)))
            sample-count (count sample)
            new-full-sample (into full-sample sample)]
        (if (= sample-size sample-count)
          new-full-sample
          (recur (- sample-size sample-count) new-full-sample))))))

(comment
  (oz/export! {:data {:values (->> (repeatedly 100000 #(rand-normal-int 0
                                                                        100
                                                                        {:sd-skew -4}))

                                   (map hash-map (repeat :x)))}
               :mark "bar"
               :encoding {:x {:bin {:binned true
                                    :step 5}
                              :field "x"}
                          :y {:aggregate "count"
                              :type "quantitative"}}}
              "normal_distribution.html"))

(defmacro runtime [body]
  `(let [start# (. System (nanoTime))
         result# ~body]
     {:runtime-ms (/ (- (. System (nanoTime))
                        start#)
                     1000000.0)
      :value result#}))

(defn create-document
  ([client index-name document]
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

;; {} - 2

;; {"a":0} - 7
;; {"a":""} - 8
;; {"a":"x"} - 9

;; {"a":0,"b":0} - 13
;; {"a":"","b":""} - 15
;; {"a":"x","b":"y"} - 17

;; {"a":0,"b":0,"c":0} - 19
;; {"a":"","b":"","c":""} - 22
;; {"a":"x","b":"y","c":"z"} - 25

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

(defn generate-field-name [size]
  (apply str
         (rand-nth lowercase-alpha)
         (take (dec size)
               (repeatedly #(rand-nth lowercase-alpha-numeric)))))

(defn generate-value [size]
  (apply str
         (take size
               (repeatedly #(rand-nth (concat lowercase-alpha-numeric
                                              symbol*))))))

(def non-zero? (complement zero?))

(defn rand-normal [min* max* & [{:keys [skew sd-skew]
                                 :or {skew 0
                                      sd-skew 0}}]]
  (assert (not (and (non-zero? skew)
                    (non-zero? sd-skew)))
          "Specify only one of 'skew' or 'sd-skew'")
  (let [mean (/ (- max* min*) 2.0)
        standard-deviation (/ (- mean min*) 3.0)
        actual-skew (if (zero? sd-skew)
                      skew
                      (* standard-deviation sd-skew))
        possibly-skewed-mean (+ mean actual-skew)
        sample-size 1]
    (first
     (clipped-normal-distribution
      sample-size possibly-skewed-mean standard-deviation min* max*))))

(defn rand-normal-int [& args]
  (Math/round (apply rand-normal args)))

(defn generate-tokens
  [generator number-of-tokens available-size unique?]
  (loop [available-size available-size
         tokens (if unique? #{} [])]
    (let [tokens-to-go (- number-of-tokens (count tokens))
          minimum-required-size-for-rest tokens-to-go]
      ;; In case of `unique?` maybe check instead if it's even possible to
      ;; generate more unique tokens.
      (if (zero? tokens-to-go)
        tokens
        (let [token-size (if (= 1 tokens-to-go)
                           available-size
                           (let [min* 1
                                 max* (- available-size
                                         minimum-required-size-for-rest)
                                 soft-max* (float (min (/ available-size tokens-to-go)
                                                       max*))
                                 desired-mean (inc (/ (- soft-max* min*)
                                                      2.0))
                                 standard-deviation (Math/abs (/ (- desired-mean min*) 3.0))
                                 s (first
                                    (clipped-normal-distribution
                                     1 desired-mean standard-deviation 1 max*))]
                             ;; (pprint {:max max*
                             ;;          :soft-max soft-max*
                             ;;          :desired-mean desired-mean
                             ;;          :s s
                             ;;          :standard-deviation standard-deviation})
                             (Math/round s)))
              token (apply str (generator token-size))]
          ;; (pprint {:tokens [(count tokens) tokens-to-go]
          ;;          :available-size available-size
          ;;          :token-size token-size
          ;;          :token token})
          (if (and unique? (contains? tokens token))
            ;; This might recur forever.
            (recur available-size tokens)
            (recur (- available-size token-size)
                   (conj tokens token))))))))

(defn generate-document [size]
  (let [document-overhead 2      ;; {}
        kv-overhead 5            ;; "":""
        additional-kv-overhead 1 ;; ,
        minimum-kv-size 7        ;; "a":"b"
        minimum-document-size (+ document-overhead minimum-kv-size)
        maximum-number-of-kvs
        (loop [available-size (- size document-overhead)
               number-of-kvs 0]
          (let [possible-additional-kv-overhead (if (pos? number-of-kvs)
                                                  1
                                                  0)
                available-size-with-another-kv (- available-size
                                                  (+ minimum-kv-size
                                                     possible-additional-kv-overhead))]
            (if (> 0 available-size-with-another-kv)
              number-of-kvs
              (recur available-size-with-another-kv
                     (inc number-of-kvs)))))
        number-of-kvs (rand-normal-int 1 maximum-number-of-kvs)
        total-overhead (+ document-overhead
                          (* number-of-kvs kv-overhead)
                          (* (- number-of-kvs 1) additional-kv-overhead))
        current-available-size (- size total-overhead)
        ;; _ (pprint [:available-size-minus-overhead current-available-size])
        minimum-token-size 1
        minimum-size-for-ks (* minimum-token-size number-of-kvs)
        minimum-size-for-vs (* minimum-token-size number-of-kvs)
        maximum-size-for-ks (- current-available-size minimum-size-for-vs)
        size-for-ks (rand-normal-int minimum-size-for-ks
                                     maximum-size-for-ks
                                     {:sd-skew -2})
        ;; _ (pprint [:size-for-ks size-for-ks])
        ks (generate-tokens generate-field-name
                            number-of-kvs
                            size-for-ks
                            true)
        size-for-vs (- current-available-size size-for-ks)
        ;; _ (pprint [:size-for-vs size-for-vs])
        vs (generate-tokens generate-value
                            number-of-kvs
                            size-for-vs
                            false)
        current-available-size (- current-available-size (reduce + (map count vs)))
        ;; _ (pprint [:available-size-minus-vs current-available-size])
        ]
    (let [size-ks (reduce + (map count ks))
          size-vs (reduce + (map count vs))
          count-ks (count ks)
          count-vs (count vs)]
      {:maximum-number-of-kvs maximum-number-of-kvs
       :number-of-kvs number-of-kvs
       :ks ks
       :vs vs
       :count-ks count-ks
       :count-vs count-vs
       :size-for-ks size-for-ks
       :size-for-vs size-for-vs
       :size-ks size-ks
       :size-vs size-vs
       :total-overhead total-overhead
       :current-available-size current-available-size
       :generated-size (+ total-overhead size-ks size-vs)
       :size size
       :document (into {} (map vector ks vs))
       :size-ok? (= size (+ total-overhead size-ks size-vs))})))

(pprint (take 1 (repeatedly #(generate-document 1000))))

;; 20 => rand 2 (- total-size minimum-size-for-rest)
;;       rand 2 (- total-size minimum-size-for-rest)

(defn generate-document-batches [documents bulk-size]
  (->> (repeatedly documents generate-document)
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

(defn run [{:keys [hosts documents bulk bulk-size index-name threads]
            :or {hosts ["http://localhost:9200"]
                 bulk false
                 bulk-size 50
                 threads 1
                 index-name "elasticsearch-stress"}}]
  (info "")
  (info "Documents:" documents)
  (info "Index name:" index-name)
  (info "Bulk size:" bulk-size)
  (info "Bulk:" bulk)
  (info "")
  (info "Starting load")
  (let [client (s/client {:hosts ["http://localhost:9200"
                                  "http://localhost:9201"]})
        pool (cp/threadpool threads :name "document-indexer")
        {total-runtime-ms :runtime-ms
         outcomes :value}
        (runtime
         (if bulk
           (->> (generate-document-batches documents bulk-size)
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
           (->> (repeatedly documents generate-document)
                (cp/pmap pool (partial process-document client index-name))
                (reduce (partial merge-with conj)
                        {:runtime-ms []
                         :status []
                         :total []
                         :successful []
                         :failed []}))))
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
    (info "Total runtime:" (format "%.2fms" total-runtime-ms))
    (cp/shutdown pool))
  (info "Ended load"))

(defn -main
  [& args]
  (comment
    (pprint (create client))
    (pprint (s/request client {:url [:_search]
                               :method :get
                               :body {:query {:match_all {}}}})))

  (run {:hosts ["http://localhost:9200"
                "http://localhost:9201"]
        :documents 10
        :threads 8
        :bulk false
        :bulk-size 10
        :index-name "elasticsearch-stress"}))
