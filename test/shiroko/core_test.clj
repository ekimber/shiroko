(ns shiroko.core-test
  (:use clojure.java.io)
  (:require [clojure.test :refer :all]
            [midje.sweet :refer :all]
            [shiroko.core :refer :all]
            [clojure.core.async :refer [<!!]]
            [criterium.core :refer [bench]]))

; TESTS SETUP
(def x (ref 2))

(defn increment-x []
  (ref-set x (inc @x)))

(defn times-2-x []
  (ref-set x (* 2 @x)))

(defn bad-txn []
  (throw (RuntimeException.)))

(defn delete-test-files []
  (doseq [f (file-seq (file "testbase"))]
    (delete-file f true)))

(defn reset-refs []
  (dosync (ref-set x 2)))

(init-db :data-dirname "testbase")

(namespace-state-changes
  [(before :facts (reset-refs))])

; TEST CASES
(fact "Simple increment transaction"
  (<!! (apply-transaction increment-x)) => 3
  @x => 3)

(fact "Failing transaction closes channel"
  (<!! (apply-transaction bad-txn)) => nil)

(fact "Writing/reading a snapshot"
  @(write-snapshot (create-snapshot [x]) "11.snapshot")
  (read-snapshot "." 11) => '(2)
  (delete-file (file "11.snapshot")))

(fact "Find last journal number before snapshot"
  (journal-to-start 10 '(3 10 23)) => 10
  (journal-to-start 9 '(3 10 23)) => 3)

(fact :bench "bench test"
  (bench (<!! (apply-transaction increment-x))))

; TEARDOWN
(delete-test-files)

