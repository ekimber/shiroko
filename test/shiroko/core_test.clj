(ns shiroko.core-test
  (:use clojure.java.io)
  (:require [clojure.test :refer :all]
            [shiroko.core :refer :all]
            [clojure.core.async :refer [<!!]]))

(def x (ref 2))

(defn increment-x []
  (ref-set x (inc @x)))

(defn bad-txn []
  (throw (RuntimeException.)))

(defn delete-test-files []
  (doseq [f (file-seq (file "testbase"))]
    (delete-file f true)))

(deftest simple-transaction
  (testing "Simple increment transaction"
    (dosync (ref-set x 2))
    (delete-test-files)
    (init-db :data-dirname "testbase")
    (is (= 3 (<!! (apply-transaction increment-x))))
    (is (= 3 @x))))

(deftest transaction-fails
  (testing "Failing transaction closes channel"
    (delete-test-files)
    (init-db :data-dirname "testbase")
    (is (nil? (<!! (apply-transaction bad-txn))))))

(deftest test-snapshot
  (testing "Writing/reading a snapshot")
    (dosync (ref-set x 2))
    @(write-snapshot (create-snapshot [x]) "11.snapshot")
    (is (= '(2) (read-snapshot "." 11)))
    (delete-file (file "11.snapshot")))