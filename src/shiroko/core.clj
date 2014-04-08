(ns ^{:author "Ed Kimber"}
  shiroko.core
  (:import (java.io File Reader FileOutputStream FileInputStream) [java.math BigDecimal])
  (:use clojure.java.io)
  (:use clojure.tools.logging)
  (:require [clojure.string :refer [join split]])
  (:require [clojure.core.async :as async :refer [put! take! >!! <!! chan go go-loop map< thread close! mult tap alt!!]])
  (:require [clojurewerkz.serialism.core :as s]))

(def txn-counter (atom 0M))

(defn ^:private enumerate-txn [txn]
  (assoc txn :id (swap! txn-counter inc)))

(defn ^:private clj-serialize
  "Serialize args as ;-separated clojure strings"
  [& items]
  (join ";" (map #(s/serialize % :clojure) items)))

(defn ^:private serialize-txn [txn]
  (clj-serialize (:fn txn) (:args txn) (:id txn)))

;; Journal reading
(defn ^:private ingest
  "Update the transaction counter and execute a transaction."
  [[f args txn-id]]
  (if-not (< txn-id @txn-counter)
    (do
      (assert (= @txn-counter txn-id) (str "Wanted " txn-id " but counter says " @txn-counter))
      (try
        (dosync (apply f args))
        (catch Exception e (warn e "Exception thrown while reading transaction.")))
      (swap! txn-counter inc))))

(defn ^:private deserialize [txn]
  (map #(s/deserialize % s/clojure-content-type) (split txn #";")))

(defn ^:private read-journal [filename]
  (with-open [rdr (reader filename)]
   (doseq [line (line-seq rdr)]
     (ingest (deserialize line)))))

(defn ^:private file-number-matcher [pattern]
  (fn [file] (second (re-matches pattern (.getName file)))))

(defn ^:private file-numbers [dir pattern]
  (map #(BigDecimal. %) (remove nil? (map (file-number-matcher pattern) (file-seq dir)))))

(defn ^:private journal-file-name [dir txn-number]
  (str dir "/" txn-number ".journal"))

(defn ^:private write-to-stream [w txn]
  (when txn
    (.write w (str txn "\n"))
    (.flush w)))

(defn ^:private journal-writer
  "Returns a channel from which it takes transactions and writes them to the given filename until the channel closes."
  [filename]
  (let [write-ch (map< serialize-txn (chan))]
    (go (with-open [w (writer filename)]
          (loop []
            (when-let [txn (<! write-ch)]
              (write-to-stream w txn)
              (recur)))))
    write-ch))

(defn ^:private txn-journaller [dir batch-size] "Journals transactions to the dir in batches of `batch-size`."
  (let [write-ch (chan)]
    (go-loop []
      (let [txn (<! write-ch) out (journal-writer (journal-file-name dir (txn :id)))]
        (>! out txn)
        (loop [i 1]
          (when (< i batch-size)
            (>! out (<! write-ch))
            (recur (inc i))))
        (close! out))
      (recur))
    write-ch))

;; Snapshot writing
(defn write-snapshot [snapshot fname]
  (future (with-open [w (writer fname)]
    (.write w snapshot)
    (.flush w))))

(defn create-snapshot
  "Serialize the persistent refs as a snapshot that will be loaded on next start."
  [ref-list] (apply clj-serialize (map deref ref-list)))

(defn snapshot-filename [id dir]
  (str dir "/" id ".snapshot"))

(defn ^:private snapshot-writer [ref-list dir snapshot-trigger]
  (fn [txn]
    (alt!!
      snapshot-trigger (write-snapshot (create-snapshot ref-list) (snapshot-filename (txn :id) dir))
      :default :noop)))

(defn ^:private execute [txn]
  (try
    (put! (:ret txn) (dosync (apply (:fn txn) (:args txn))))
    (catch Exception e
      (warn e "Exception thrown while executing transaction.")
      (close! (:ret txn)))))

(defn ^:private txn-executor "Take and execute transactions on channel."
  ([input]
    (loop [] (execute (<!! input)) (recur)))
  ([input snapshot]
    (if (nil? snapshot)
      (txn-executor input)
      (loop [txn (<!! input)]
        (execute txn)
        (snapshot txn)
        (recur (<!! input))))))

(defn read-snapshot [dir num]
  (deserialize (slurp (str dir "/" num ".snapshot"))))

(defn latest-snapshot-id [dir]
  (if-let [snap-nums (seq (file-numbers dir #"(\d+)\.snapshot\z"))]
    (apply max snap-nums)))

(defn apply-snapshot [snapshot ref-list]
  (map #(dosync (ref-set %1 %2)) ref-list snapshot))

(defn journals-from [snapshot-num journal-nums]
  (let [sorted (sort journal-nums)]
    (filter (fn [x] (>= x (last (filter #(<= % snapshot-num) sorted)))) sorted)))

(def input-ch (map< enumerate-txn (chan 10)))

(defn init-db
  "Initialise the persistence base, reading and executing all persisted transactions.
  Options:
    ref-list: list of refs to persist in snapshots
    batch-size: number of transactions per journal
    data-"
  [& {:keys [data-dir ref-list batch-size]
      :or {data-dir "base"
           ref-list nil
           batch-size 1000}}]
  (let [dir (File. data-dir)]
    (if (.exists dir)
      (when-not
        (.isDirectory dir)
        (throw (RuntimeException. (str "\"" dir "\" must be a directory"))))
      (when-not
        (.mkdir dir)
        (throw (RuntimeException. (str "Can't create database directory \"" dir "\"")))))
    (reset! txn-counter 0M)
    ;Read snapshot
    (when-let [last-txn-id (latest-snapshot-id dir)]
      (apply-snapshot (read-snapshot dir last-txn-id) ref-list)
      (reset! txn-counter (inc last-txn-id)))
    ;Read journal
    (doseq [n (journals-from @txn-counter (file-numbers dir #"(\d+)\.journal\z"))]
      (read-journal (journal-file-name dir n)))
    ;Start writer
    (let [trigger (chan) input-mult (mult input-ch)]
      (tap input-mult (txn-journaller dir batch-size)) ; pushes input transactions into the journaller
      (thread (txn-executor (tap input-mult (chan)) (if ref-list (snapshot-writer ref-list dir trigger))))
      {:snapshot-trigger trigger :ref-list ref-list})))

(defn apply-transaction
  "Persist and apply a transaction, returning a channel that will contain the result. "
  [txn-fn & args]
  (assert (fn? txn-fn) "Transaction to apply must be a fn.")
  (let [c (chan)]
    (put! input-ch {:fn txn-fn :args args :ret c})
    c))

(defn take-snapshot "Pauses execution and takes a snapshot."
  [prevalent-system]
  (put! (prevalent-system :snapshot-trigger) :go))