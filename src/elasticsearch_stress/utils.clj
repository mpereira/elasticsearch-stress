(ns elasticsearch-stress.utils
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as s])
  (:import (java.io BufferedReader File FileOutputStream)
           (java.util Properties)
           (org.apache.commons.io FileUtils)))

(def non-zero? (complement zero?))

(defn between? [min* max* n]
  (<= min* n max*))

(defn pprint-str [x] (s/trim (with-out-str (pprint x))))

(defn string->int
  [^String s]
  (edn/read-string s))

(defn jar-path
  "Returns the full path of the JAR file in which this function is invoked."
  [& [ns]]
  (-> (or ns (class *ns*))
      (.getProtectionDomain)
      (.getCodeSource)
      (.getLocation)
      (.toURI)
      (.getPath)))

(defn properties
  "Returns the properties for a given x or namespace."
  [x]
  (when-let [pom-properties (io/resource (str "META-INF/maven"
                                              "/" (or (namespace x) (name x))
                                              "/" (name x)
                                              "/" "pom.properties"))]
    (with-open [stream (io/input-stream pom-properties)]
      (let [properties (doto (Properties.) (.load stream))]
        {:artifact-id (.getProperty properties "artifactId")
         :group-id (.getProperty properties "groupId")
         :revision (.getProperty properties "revision")
         :version (.getProperty properties "version")}))))

(defmacro runtime
  "Returns a map with the return value from body and its runtime information."
  [& body]
  `(let [start# (. System (nanoTime))
         result# (do ~@body)
         runtime-ms# (/ (- (. System (nanoTime))
                           start#)
                        1000000.0)]
     {:runtime-ms runtime-ms#
      :runtime-s (/ runtime-ms# 1000)
      :value result#}))

(defn lazy-file-seq
  "Create a lazy seq of lines from given file."
  [filename]
  (letfn [(helper [^BufferedReader r]
            (lazy-seq (if-let [line (.readLine r)]
                        (do (cons line (helper r)))
                        (do (.close r) nil))))]
    (helper (io/reader filename))))

(defn lazy-directory-list-seq
  "Return a lazy seq of files in a directory"
  [file]
  (rest (file-seq (io/file file))))

(defn truncate-file
  "Truncates file to zero bytes."
  [^File file]
  (with-open [chan (.getChannel (FileOutputStream. file true))]
    (.truncate chan 0)))

(defn files-equal?
  "Returns true if two files have the same content, false otherwise."
  [f1 f2]
  (let [f1-seq (lazy-file-seq f1)
        f2-seq (lazy-file-seq f2)
        combined-seq (map vector f1-seq f2-seq)]
    (reduce (fn [found-different-terms? [f1-term f2-term]]
              (if (= f1-term f2-term)
                found-different-terms?
                (reduced false)))
            true
            combined-seq)))

(def byte-size-units
  "TODO: docstring."
  {:b 1
   :kb 1000
   :mb (Math/pow 1000 2)
   :gb (Math/pow 1000 3)
   :tb (Math/pow 1000 4)
   :kib 1024
   :mib (Math/pow 1024 2)
   :gib (Math/pow 1024 3)
   :tib (Math/pow 1024 4)})

(def human-regex
  "TODO: docstring."
  (let [units-regex (->> (keys byte-size-units)
                         (map name)
                         (s/join "|")
                         (#(str "(" % ")?")))
        size-regex "((?:\\d+\\.)?\\d+)"
        regex-string (str "(?i)" size-regex "\\s*" units-regex "\\b")]
    (re-pattern regex-string)))

(defn human->duration
  "TODO: docstring."
  [human]
  human)

(defn human->bytes
  "TODO: docstring."
  [human]
  (let [[match? number-string unit-string] (re-find human-regex human)
        number (edn/read-string number-string)
        unit (and unit-string (keyword (s/lower-case unit-string)))]
    (assert match? (str "'" human "' isn't a valid unit pattern"))
    (* number (or (get byte-size-units unit) 1))))

(defn bytes->human
  "TODO: docstring."
  [bytes* {:keys [in]}]
  (let [unit (keyword (s/lower-case (name in)))]
    (assert
     (contains? byte-size-units unit)
     (str "'" in "'" " isn't a valid unit. Valid units are: "
          (keys byte-size-units)))
    (* bytes* (get byte-size-units unit))))
