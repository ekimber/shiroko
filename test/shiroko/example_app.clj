(ns ^{:author "Ed Kimber"} shiroko.example-app
  (:use [shiroko.core])
  (:require [clojure.core.async :as async :refer [<!! thread]])
  (:require [clj-time.format :as f])
  (:require [clj-time.local :as l]))

;;; APP CODE
(def msgs (ref []))

; Current time as string
(def iso-date (f/formatter "hh:mm:ss.SSS"))
(defn now-str [] (f/unparse iso-date (l/local-now)))

(defn write-msg [name msg time]
  (ref-set msgs (conj @msgs [name msg time])))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [base (init-db :ref-list [msgs] :batch-size 4)]
    (dotimes [n 10]
      (<!! (apply-transaction base write-msg "Bob" n (now-str))))
    (take-snapshot base)
    (dotimes [n 10]
      (thread (<!! (apply-transaction base write-msg "Bob" n (now-str))))))
  (Thread/sleep 500))
