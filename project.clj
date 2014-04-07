(defproject shiroko "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/core.async "0.1.278.0-76b25b-alpha"]
                 [clj-time "0.6.0"]
                 [clojurewerkz/serialism "1.1.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [criterium "0.4.3"]]
  :plugins [[lein-marginalia "0.7.1"]
            [lein-midje "3.1.3"]]
  :main ^:skip-aot shiroko.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[midje "1.6.3"]]}}
  :test-selectors {:default (complement :bench)
                   :bench :bench
                   :all (constantly true)})
