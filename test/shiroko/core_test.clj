(ns ^{:author "Ed Kimber"} shiroko.core-test
  (:use clojure.java.io)
  (:require [clojure.test :refer :all]
            [midje.sweet :refer :all]
            [shiroko.core :refer :all]
            [clojure.core.async :refer [<!! >!! put! chan]]
            [criterium.core :refer [bench]]))

; TESTS SETUP
(def x (ref 2))

(defn increment-x []
  (ref-set x (inc @x)))

(defn times-2-x []
  (ref-set x (* 2 @x)))

(defn bad-txn []
  (throw (RuntimeException.)))

(defn delete-test-files [dir]
  (doseq [f (file-seq (file dir))]
    (delete-file f true)))

(defn reset-refs []
  (dosync (ref-set x 2)))

(delete-test-files "testbase")
(def base (init-db :data-dir "testbase" :batch-size 4))

; TEST CASES
(fact "Enumerate adds id key"
  (let [ch (chan)]
    (put! ch {})
    (<!! (enumerate ch 4)) => {:id 4}))

(fact "Simple increment transaction"
  (reset-refs)
  (<!! (apply-transaction base increment-x)) => 3
  @x => 3)

(fact "Failing transaction closes channel"
  (<!! (apply-transaction base bad-txn)) => nil)

(fact "Writing/reading a snapshot"
  @(write-snapshot (create-snapshot [(ref 2)]) "11.snapshot")
  (read-snapshot "." 11) => '(2)
  (delete-file (file "11.snapshot")))

(fact "Find journal sequence for snapshot number."
  (journals-from 10 '(3 10 23)) => '(10 23)
  (journals-from 9 '(23 2 12)) => '(2 12 23)
  (journals-from 0 '(1 9 15)) => '(1 9 15))

(fact "Loads serialized journal"
  (delete-test-files "t2")
  (let [db (init-db :data-dir "t2" :batch-size 4)]
    (reset-refs)
    (dotimes [_ 10] (<!! (apply-transaction db increment-x)))
    (let [new-x @x]
      (reset-refs)
      @x => 2
      (init-db :data-dir "t2" :batch-size 4)
      @x => new-x)))

(fact "Loads snapshot"
  (delete-test-files "test3")
  (let [db (init-db :data-dir "test3" :batch-size 4 :ref-list [x])]
    (reset-refs)
    (dotimes [_ 10] (<!! (apply-transaction db increment-x)))
    (take-snapshot db)
    (<!! (apply-transaction db increment-x))
    (delete-file  (file "test3/0.journal"))
    (init-db :data-dir "test3" :batch-size 4 :ref-list [x])))
    
    
(fact :bench "bench test"
  (bench (<!! (apply-transaction base increment-x))))

; TEARDOWN
(delete-test-files "testbase")

