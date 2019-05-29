(ns performance-benchmark-framework.elasticsearch
  (:require [cheshire.core :as json]
            [clojure.string :as string]
            [qbits.spandex :as s]))

(defn client [& args]
  (apply s/client args))

(defn request [& args]
  (apply s/request args))

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

(defn create-documents [client index-name documents]
  ((make-bulk-operation (fn [document] [{:index {}} document]))
   client index-name documents))
