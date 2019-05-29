(ns performance-benchmark-framework.core
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [com.climate.claypoole :as cp]
            [oz.core :as oz]
            [performance-benchmark-framework.elasticsearch :as elasticsearch]
            [performance-benchmark-framework.generators :as generators]
            [performance-benchmark-framework.statistics :as statistics]
            [performance-benchmark-framework.utils :refer [pprint-str]]
            [taoensso.timbre :refer [info]]))

(defmacro runtime [body]
  `(let [start# (. System (nanoTime))
         result# ~body]
     {:runtime-ms (/ (- (. System (nanoTime))
                        start#)
                     1000000.0)
      :value result#}))

(defn process-bulk-batch [client index-name batch]
  (let [{:keys [runtime-ms]
         {:keys [hosts]
          {:keys [took errors items]} :body
          bulk-status :status} :value}
        (runtime (elasticsearch/create-documents client index-name batch))
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
        (runtime (elasticsearch/create-document client index-name document))]
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
  (let [{:keys [mapping size-remaining-for-values]} (generators/generate-mapping
                                                     document-size)]
    (info "")
    (info "Documents:" documents)
    (info "Document size:" document-size)
    (info "Index name:" index-name)
    (info "Bulk size:" bulk-size)
    (info "Bulk:" bulk)
    (info "Mapping:" (pprint-str mapping))
    (info "")
    (info "Starting load")
    (let [client (elasticsearch/client {:hosts ["http://localhost:9200"
                                                "http://localhost:9201"]})
          pool (cp/threadpool threads :name "document-indexer")
          {total-runtime-ms :runtime-ms
           outcomes :value}
          (runtime
           (if bulk
             (->> (generators/generate-document-batches
                   documents
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
                                          #(generators/generate-document
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
                     (update :runtime-ms statistics/statistics)
                     (update :bulk-status frequencies)
                     (update :status frequencies)
                     (update :result frequencies)
                     (update :took statistics/statistics)
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
  (let [{:keys [fields size-remaining-for-values]} (generators/generate-fields 500)]
    (pprint
     (take 5
           (repeatedly
            #(generators/generate-document fields size-remaining-for-values)))))

  (pprint
   (elasticsearch/request
    (elasticsearch/client {:hosts ["http://localhost:9200"
                                   "http://localhost:9201"]})
    {:url [:elasticsearch-stress]
     :method :get}))
  (pprint
   (elasticsearch/request
    (elasticsearch/client {:hosts ["http://localhost:9200"
                                   "http://localhost:9201"]})
    {:url [:_search]
     :method :get
     :body {:query {:match_all {}}}}))
  (pprint
   (elasticsearch/create-document
    (elasticsearch/client {:hosts ["http://localhost:9200"
                                   "http://localhost:9201"]})
    :elasticsearch-stress
    {:foo "foo"
     :bar "bar"}))
  (pprint
   (elasticsearch/request
    (elasticsearch/client {:hosts ["http://localhost:9200"
                                   "http://localhost:9201"]})
    {:url [:elasticsearch-stress]
     :method :delete}))

  (oz/export!
   {:data {:values (->> (repeatedly
                         100000
                         #(random/rand-normal-int
                           35
                           754
                           {:sd (/ (random/standard-deviation-from-range 35 754)
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
                                    1000000
                                    #(random/rand-exponential {:max-value 100}))
                                   (map hash-map (repeat :x)))}
               :mark "bar"
               :encoding {:x {:bin {:binned true
                                    :step 5}
                              :field "x"}
                          :y {:aggregate "count"
                              :type "quantitative"}}}
              "exponential_distribution.html"))

(def cli-options
  [[nil "--bulk" "Whether to use bulk requests or not. Defaults to true"
    :default true
    :id :bulk]
   [nil "--bulk-size BULK_SIZE" "Size of bulk requests"
    :default 100
    :parse-fn #(Integer/parseInt %)
    :id :bulk-size]
   [nil "--document-size DOCUMENT_SIZE" "Size in bytes of documents"
    :parse-fn #(Integer/parseInt %)
    :id :document-size]
   [nil "--documents DOCUMENTS" "Number of documents to index"
    :parse-fn #(Integer/parseInt %)
    :id :documents]
   [nil "--hosts HOSTS" "List of comma-separated Elasticsearch hosts"
    :parse-fn #(string/split % #",")
    :default ["http://localhost:9200"]
    :id :hosts]
   [nil "--index-name INDEX_NAME" "Where documents will be indexed to"
    :default "elasticsearch-stress"
    :id :index-name]
   [nil "--threads THREADS" "Number of indexing threads to spawn"
    :default 2
    :parse-fn #(Integer/parseInt %)
    :id :threads]
   ["-v" "--version" "Show version"
    :id :version]
   ["-h" "--help" "Show help"
    :id :help]])

(def version "0.0.1")

(def program-name "elasticsearch-stress")

(defn usage-message [summary & [{:keys [show-preamble?]
                                 :or {show-preamble? true}}]]
  (let [preamble (when show-preamble?
                   (->> [(str program-name " " version)
                         ""
                         "elasticsearch-stress is a stress tool for Elasticsearch."
                         ""]
                        (string/join \newline)))]
    (->> [preamble
          "Usage:"
          (str "  " program-name " " "[OPTIONS]")
          ""
          "Options:"
          summary]
         (string/join \newline))))

(defn error-message [{:keys [raw-args errors] :as parsed-opts}]
  (str "The following errors occurred while parsing your command:"
       " "
       "`" program-name " " (apply str raw-args) "`"
       "\n\n"
       (string/join \newline errors)
       "\n\n"
       "Run `elasticsearch-stress --help` for more information"))

(defn valid-command? [{:keys [arguments summary options] :as parsed-opts}]
  (empty? arguments))

(defn dispatch-command [{:keys [arguments summary options raw-args] :as parsed-opts}]
  (cond
    (or (:help options)
        (contains? (set arguments) "help")) {:stdout (usage-message summary)
                                             :return-code 0}
    (:version options) {:stdout version
                        :return-code 0}
    (valid-command? parsed-opts) (run options)
    :else {:stdout
           (str "Invalid command: "
                "`" program-name (when raw-args (apply str " " raw-args)) "`"
                \newline
                (usage-message summary {:show-preamble? false}))
           :return-code 1}))

(comment
  (run {:bulk true
        :bulk-size 500
        :document-size 10000
        :documents 5000
        :hosts ["http://localhost:9200" "http://localhost:9201"]
        :index-name "elasticsearch-stress"
        :threads 4})
  (-main "run")
  (-main "--help")
  (-main "--bulk"
         "--bulk-size" "500"
         "--document-size" "1000"
         "--documents" "5000"
         "--hosts" "http://localhost:9200,http://localhost:9201"
         "--index-name" "elasticsearch-stress"
         "--threads" "4"))

(defn -main
  [& args]
  (let [{:keys [errors] :as parsed-opts} (assoc (parse-opts args cli-options)
                                                :raw-args args)]
    (println "********************************************************************************")
    (pprint (dissoc parsed-opts :summary))
    (println "********************************************************************************")
    (if errors
      (println (error-message parsed-opts))
      (let [{:keys [stdout return-code]} (dispatch-command parsed-opts)]
        (println stdout)
        ;; (System/exit return-code)
        ))))
