(ns shiroko.core-test
  (:use clojure.java.io)
  (:require [clojure.test :refer :all]
            [shiroko.core :refer :all]
            [clojure.core.async :refer [<!!]]
            [criterium.core :refer [bench]]))

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

(defn db-setup [f]
  (init-db :data-dirname "testbase")
  (f)
  (delete-test-files))

(defn reset-refs [f]
  (dosync (ref-set x 2))
  (f))

(use-fixtures :once db-setup)
(use-fixtures :each reset-refs)

(deftest simple-transaction
  (testing "Simple increment transaction"
    (is (= 3 (<!! (apply-transaction increment-x))))
    (is (= 3 @x))))

(deftest transaction-fails
  (testing "Failing transaction closes channel"
    (is (nil? (<!! (apply-transaction bad-txn))))))

(deftest test-snapshot
  (testing "Writing/reading a snapshot")
    @(write-snapshot (create-snapshot [x]) "11.snapshot")
    (is (= '(2) (read-snapshot "." 11)))
    (delete-file (file "11.snapshot")))

(deftest ^:bench bench-test
  (testing "bench test")
  (bench (<!! (apply-transaction increment-x))))
