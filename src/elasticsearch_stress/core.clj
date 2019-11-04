(ns elasticsearch-stress.core
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as s]
            [clojure.tools.cli :refer [parse-opts]]
            [com.climate.claypoole :as cp]
            [elasticsearch-stress.elasticsearch :as elasticsearch]
            [elasticsearch-stress.generators :as generators]
            [elasticsearch-stress.statistics :as statistics]
            [elasticsearch-stress.utils :refer [pprint-str
                                                runtime
                                                human->duration
                                                human->bytes
                                                jar-path
                                                properties]]
            [taoensso.timbre :as timbre :refer [info]]))


(timbre/merge-config! {:timestamp-opts {:pattern "yyyy-MM-dd HH:mm:ss'Z'"}})

;; GraalVM does not support reflection.
;; Commenting for Emacs' cljr-* functions to work.
;; (set! *warn-on-reflection* true)

(defonce program-properties (properties 'elasticsearch-stress))

(def program-name (:artifact-id program-properties))

(def program-description
  (str program-name
       " "
       "is a tool for performance testing Elasticsearch clusters"))

(def program-jar-path (jar-path))

(def program-version (:version program-properties))

(def program-revision (:revision program-properties))

(def program-short-revision (subs program-revision 0 8))

(def program-version-and-revision
  (str program-version " (" program-short-revision ")"))

(def program-command (str "java -jar " program-jar-path))

(def cli-options
  {:help [["-v" "--version" "Show version"
           :id :version]
          ["-h" "--help" "Show help"
           :id :help]]
   :write [[nil "--bulk" "Whether or not to use bulk requests"
            :default true
            :id :bulk]
           [nil
            "--bulk-documents BULK_DOCUMENTS"
            "Maximum number of documents in each bulk request"
            :default 1000
            :parse-fn #(Integer/parseInt %)
            :id :bulk-documents]
           [nil
            "--workload-size WORKLOAD_SIZE"
            "Total number of bytes/megabytes/gigabytes to write (e.g.: \"10GiB\")"
            :parse-fn human->bytes
            :id :workload-size]
           [nil
            "--workload-duration WORKLOAD_DURATION"
            "Amount of time the workload will run for (e.g.: \"2h\")"
            :parse-fn human->duration
            :id :workload-duration]
           [nil
            "--workload-documents WORKLOAD_DOCUMENTS"
            "Total number of documents to be indexed"
            :default 100000
            :id :workload-documents]
           [nil "--hosts HOSTS" "List of comma-separated Elasticsearch hosts"
            :parse-fn #(s/split % #",")
            :default ["http://localhost:9200"]
            :id :hosts]
           [nil "--threads THREADS" "Number of indexing threads to spawn"
            :default 2
            :parse-fn #(Integer/parseInt %)
            :id :threads]
           [nil "--index-name INDEX_NAME" "Where documents will be indexed to"
            :default "elasticsearch-stress"
            :id :index-name]
           [nil
            "--index INDEX"
            "Path to a JSON file with index settings/mappings/aliases"
            :default "elasticsearch_stress.json"
            :id :index]]})

(def available-subcommands (set (keys cli-options)))

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

(defn run-write [{:keys [bulk
                         bulk-documents
                         workload-size
                         workload-duration
                         workload-documents
                         hosts
                         index-name
                         index
                         threads]}]
  (let [default-number-of-shards 1
        default-number-of-replicas 1
        {:keys [mapping] :as index} (generators/generate-index
                                     default-number-of-shards
                                     default-number-of-replicas)]
    (info (format "%-19s" "Bulk:") bulk)
    (info (format "%-19s" "Bulk documents:") bulk-documents)
    (when workload-size
      (info (format "%-19s" "Workload size:") workload-size))
    (when workload-duration
      (info (format "%-19s" "Workload duration:") workload-duration))
    (info (format "%-19s" "Workload documents:") workload-documents)
    (info (format "%-19s" "Hosts:") hosts)
    (info (format "%-19s" "Threads:") threads)
    (info (format "%-19s" "Index name:") index-name)
    (info (str "Index:\n" (pprint-str index)))
    (info "Starting load")
    (let [client (elasticsearch/client {:hosts hosts})
          pool (cp/threadpool threads :name "document-indexer")
          {total-runtime-ms :runtime-ms
           outcomes :value}
          (runtime
           (if bulk
             (->> (generators/generate-document-batches
                   workload-size
                   workload-documents
                   bulk-documents
                   mapping)
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
             (->> (partial generators/generate-document mapping)
                  (repeatedly workload-documents)
                  (cp/pmap pool (partial process-document client index-name))
                  (reduce (partial merge-with conj)
                          {:runtime-ms []
                           :status []
                           :total []
                           :successful []
                           :failed []}))))
          report (-> outcomes
                     (update :runtime-ms statistics/statistics)
                     (update :bulk-status frequencies)
                     (update :status frequencies)
                     (update :result frequencies)
                     (update :took statistics/statistics)
                     (update :total (partial reduce +))
                     (update :successful (partial reduce +))
                     (update :failed (partial reduce +)))]
      (info (str "Finished load\n" (pprint-str report)))
      (info "Total Elasticsearch 'took' runtime:"
            (format "%dms" (get-in report [:took :total])))
      (info "Total observed runtime:" (format "%.2fms" total-runtime-ms))
      (cp/shutdown pool))
    (info "Ended load")))

(def runners
  {:write run-write})

(defn usage-message [summary subcommand & [{:keys [show-preamble?]
                                            :or {show-preamble? true}}]]
  (let [preamble
        (when show-preamble?
          (->> [(str program-name " " program-version-and-revision)
                ""
                program-description
                ""]
               (s/join \newline)))]
    (->> [preamble
          "Usage"
          (str "  " program-command " [SUBCOMMAND] [OPTIONS]")
          ""
          "Options"
          summary
          ""
          "Subcommands"
          (->> (dissoc cli-options :help)
               (map (fn [[subcommand options]]
                      (str "  " (name subcommand) \newline
                           (s/join \newline
                                   (map #(str "    " (nth % 1)) options)))))
               (s/join "\n\n"))]
         (s/join \newline))))

(defn error-message [{:keys [raw-args errors] :as parsed-opts} subcommand]
  (str "The following errors occurred while parsing your command:"
       " "
       "`" program-command " " (s/join " " raw-args) "`"
       "\n\n"
       (s/join \newline errors)
       "\n\n"
       "Run `"
       program-command " --help"
       "` for more information"))

(defn valid-command? [{:keys [arguments summary options errors] :as parsed-opts}]
  (empty? errors))

(defn dispatch-command
  [{:keys [arguments summary options raw-args] :as parsed-opts} subcommand]
  (cond
    (or (nil? subcommand)
        (= :help subcommand)
        (:help options)
        (contains? (set arguments) "help")) {:stdout (usage-message summary
                                                                    subcommand)
                                             :return-code 0}
    (:version options) {:stdout program-version-and-revision
                        :return-code 0}
    (and subcommand
         (valid-command? parsed-opts)) {:stdout
                                        ((get runners subcommand) options)
                                        :return-code 0}
    :else {:stdout
           (str "Invalid command: "
                "`"
                program-command (when raw-args (str " " (s/join " " raw-args)))
                "`"
                \newline
                (usage-message summary subcommand {:show-preamble? false}))
           :return-code 1}))

(defn -main
  "Entry-point to search-engine-indexer."
  [& args]
  (let [{:keys [options arguments] :as parsed-opts}
        (parse-opts args (:help cli-options))
        ^String possible-subcommand (first args)
        subcommand-likely? (and (not (empty? possible-subcommand))
                                (not (.startsWith possible-subcommand "--"))
                                (= possible-subcommand (first arguments)))]
    (if-let [subcommand (keyword (if subcommand-likely?
                                   possible-subcommand
                                   "help"))]
      (if (contains? available-subcommands subcommand)
        (let [{:keys [arguments errors] :as parsed-opts}
              (assoc (parse-opts args (get cli-options subcommand))
                     :raw-args args)]
          (if errors
            (println (error-message parsed-opts subcommand))
            (if (< 1 (count arguments))
              (do
                (println "More than one subcommand given:" arguments)
                (println "Available subcommands:" available-subcommands))
              (let [{:keys [stdout return-code]}
                    (dispatch-command parsed-opts subcommand)]
                (println stdout)
                ;; (System/exit return-code)
                ))))
        (do
          (println "Subcommand" (name subcommand) "doesn't exist")
          (println "Available subcommands:" available-subcommands)))
      (let [{:keys [stdout return-code]}
            (dispatch-command parsed-opts nil)]
        (when-not (empty? stdout)
          (println stdout))
        ;; FIXME: uncomment this.
        ;; (System/exit return-code)
        ))))
