(defproject elasticsearch-stress "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]

                 [cc.qbits/spandex "0.7.1"]
                 [cheshire "5.9.0"]
                 [com.climate/claypoole "1.1.4"]
                 [com.taoensso/timbre "4.10.0"]
                 [kixi/stats "0.5.2"]
                 [org.clojure/tools.cli "0.4.2"]]
  :target-path "target/%s"
  :main elasticsearch-stress.core
  :profiles
  {:dev
   {:source-paths ["dev"]
    :dependencies [;; Adding this early in the dependency list so that its
                   ;; subdependencies are overriden.
                   [metasoarous/oz "1.5.6"]
                   ;; https://mvnrepository.com/artifact/com.carrotsearch.randomizedtesting/randomizedtesting-runner
                   [com.carrotsearch.randomizedtesting/randomizedtesting-runner "2.7.4"]]
    :plugins [[lein-ancient "0.6.15"]
              [lein-bikeshed "0.5.2"]
              [lein-cljfmt "0.6.4"]]}
   :uberjar {:aot :all}})
