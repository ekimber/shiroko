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
    (delete-test-files)
    (init-db "testbase")
    (assert (= 3 (<!! (apply-transaction increment-x))))
    (assert (= 3 @x))))

(deftest transaction-fails
  (testing "Failing transaction closes channel"
    (delete-test-files)
    (init-db "testbase")
    (assert (nil? (<!! (apply-transaction bad-txn))))))
