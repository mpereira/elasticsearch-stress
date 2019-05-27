(defproject performance-benchmark-framework "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]

                 ;; Adding this early in the dependency list so that its
                 ;; subdependencies are overriden.
                 [metasoarous/oz "1.5.6"]

                 [cc.qbits/spandex "0.7.0-beta3"]
                 [cheshire "5.8.1"]
                 [com.climate/claypoole "1.1.4"]
                 [com.taoensso/timbre "4.10.0"]
                 [kixi/stats "0.5.0"]
                 [org.clojure/tools.cli "0.4.2"]]
  :main ^:skip-aot performance-benchmark-framework.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
