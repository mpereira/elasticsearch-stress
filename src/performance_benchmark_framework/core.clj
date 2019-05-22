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

(defn pprint-str [x] (with-out-str (pprint x)))

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

(def generate-field-language (concat lowercase-alpha lowercase-alpha-numeric))

(def generate-field-language-size (count (set generate-field-language)))

(defn generate-field [size]
  {:pre [(or (zero? size) (pos? size))]}
  (apply str
         (when (pos? size) (rand-nth lowercase-alpha))
         (take (dec size)
               (repeatedly #(rand-nth lowercase-alpha-numeric)))))

(def generate-value-language (concat lowercase-alpha-numeric symbol*))

(def generate-value-language-size (count (set generate-value-language)))

(defn generate-value [size]
  {:pre [(or (zero? size) (pos? size))]}
  (apply str (take size (repeatedly #(rand-nth generate-value-language)))))

(def generate-foo-language (range 20))

(def generate-foo-language-size (count (set generate-foo-language)))

(defn generate-foo [size]
  {:pre [(or (zero? size) (pos? size))]}
  (apply str (take size (repeatedly #(rand-nth generate-foo-language)))))

(def generator-language-sizes
  {generate-field generate-field-language-size
   generate-value generate-value-language-size
   generate-foo generate-foo-language-size})

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
  [generator number-of-tokens available-size & [{:keys [unique?]
                                                 :or {unique? false}}]]
  (let [generator-language-size (get generator-language-sizes generator)]
    ;; TODO: check all possible combinations of size using language.
    ;;
    ;; Also: generate-tokens can only generate 26 distinct single "character"
    ;; tokens because its tokens must start with a lowercase-alpha.
    (if (and unique? (> number-of-tokens generator-language-size))
      (do
        (info "Can't generate" number-of-tokens "tokens with language of size" generator-language-size)
        (if unique? #{} []))
      (loop [available-size available-size
             tokens (if unique? #{} [])]
        (let [minimum-token-size 1
              tokens-to-go (- number-of-tokens (count tokens))]
          (if (zero? tokens-to-go)
            tokens
            (let [minimum-required-size (* minimum-token-size tokens-to-go)
                  minimum-required-size-for-rest (- minimum-required-size minimum-token-size)
                  token-size (if (= 1 tokens-to-go)
                               available-size
                               (let [min* minimum-token-size
                                     max* (- available-size minimum-required-size-for-rest)
                                     desired-mean (inc (/ (- max* min*) 2.0))
                                     standard-deviation (Math/abs (/ (- desired-mean min*) 3.0))
                                     s (first
                                        (clipped-normal-distribution
                                         1 desired-mean standard-deviation min* max*))]
                                 (Math/round s)))
                  token (generator token-size)]
              (if (and unique? (contains? tokens token))
                (recur available-size tokens)
                (recur (- available-size token-size)
                       (conj tokens token))))))))))

(defn generate-fields [document-size]
  (let [document-overhead 2      ;; {}
        minimum-token-size 1     ;; a
        kv-overhead 5            ;; "":""
        additional-kv-overhead 1 ;; ,
        minimum-kv-size 7        ;; "a":"b"
        minimum-document-size (+ document-overhead minimum-kv-size)
        maximum-number-of-kvs
        (loop [available-size (- document-size document-overhead)
               number-of-kvs 0]
          (let [possible-additional-kv-overhead (if (pos? number-of-kvs)
                                                  additional-kv-overhead
                                                  0)
                available-size-with-another-kv (- available-size
                                                  (+ minimum-kv-size
                                                     possible-additional-kv-overhead))]
            (if (> 0 available-size-with-another-kv)
              number-of-kvs
              (recur available-size-with-another-kv
                     (inc number-of-kvs)))))
        number-of-kvs (rand-normal-int 1 maximum-number-of-kvs {:sd-skew -2})
        total-overhead (+ document-overhead
                          (* number-of-kvs kv-overhead)
                          (* (- number-of-kvs 1) additional-kv-overhead))
        current-available-size (- document-size total-overhead)
        minimum-size-for-ks (* minimum-token-size number-of-kvs)
        minimum-size-for-vs minimum-size-for-ks
        maximum-size-for-ks (- current-available-size minimum-size-for-vs)
        size-for-ks (rand-normal-int minimum-size-for-ks
                                     maximum-size-for-ks
                                     {:sd-skew -2})
        size-for-vs (- current-available-size size-for-ks)]
    {:fields (generate-tokens generate-field number-of-kvs size-for-ks {:unique? true})
     :size-remaining-for-values size-for-vs}))

(defn generate-mapping [document-size]
  (let [{:keys [fields size-remaining-for-values]} (generate-fields document-size)]
    {:mapping {:properties (reduce (fn [properties field]
                                     (assoc properties field {:type "keyword"}))
                                   {}
                                   fields)}
     :size-remaining-for-values size-remaining-for-values}))

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

  (oz/export! {:data {:values (->> (repeatedly 100000 #(rand-normal-int 0
                                                                        100
                                                                        {:sd-skew -2}))

                                   (map hash-map (repeat :x)))}
               :mark "bar"
               :encoding {:x {:bin {:binned true
                                    :step 5}
                              :field "x"}
                          :y {:aggregate "count"
                              :type "quantitative"}}}
              "normal_distribution.html"))

(defn -main
  [& args]
  (run {:bulk true
        :bulk-size 500
        :document-size 1000
        :documents 5000
        :hosts ["http://localhost:9200" "http://localhost:9201"]
        :index-name "elasticsearch-stress"
        :threads 4}))
