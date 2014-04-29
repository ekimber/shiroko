(defproject ekimber/shiroko "0.1.2"
  :description "Clojure Data Prevalence Library"
  :url "https://github.com/ekimber/shiroko"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/core.async "0.1.278.0-76b25b-alpha"]
                 [clojurewerkz/serialism "1.1.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [criterium "0.4.3" :scope "test"]
                 [clj-time "0.6.0" :scope "test"]]
                 :plugins [[lein-marginalia "0.7.1"]
            [lein-midje "3.1.3"]]
  :main ^:skip-aot shiroko.example-app
  :repl-options {:init-ns shiroko.core}
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[midje "1.6.3"]]}})
