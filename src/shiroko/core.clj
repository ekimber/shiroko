(ns ^{:author "Ed Kimber"}
  shiroko.core
  (:import (java.io File Reader FileOutputStream FileInputStream) [java.math BigDecimal])
  (:use clojure.java.io)
  (:use clojure.tools.logging)
  (:require [clojure.string :refer [join split]])
  (:require [clojure.core.async :as async :refer [put! take! >!! <!! chan go go-loop map< thread close! mult tap alt!]])
  (:require [clj-time.format :as f])
  (:require [clj-time.local :as l])
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

(def input-ch (map< enumerate-txn (chan 10)))
(def input-mult (mult input-ch))
(def snapshot-await (chan))
(def exec-await (chan))
(def snapshot-trigger (chan))

(defn ^:private execute [txn]
  (try
    (put! (:ret txn) (dosync (apply (:fn txn) (:args txn))))
    (catch Exception e
      (warn e "Exception thrown while executing transaction.")
      (close! (:ret txn)))))

(defn ^:private txn-executor "Take and execute transactions on channel."
  [in]
    (thread
      (loop []
        (execute (<!! in))
        (>!! snapshot-await :ready)
        (recur))))

;; Journal reading
(defn ^:private ingest
  "Update the transaction counter and execute a transaction."
  [[f args txn-id]]
  (assert (= (swap! txn-counter inc) txn-id) (str "Missing transaction " txn-id "?"))
  (try
    (dosync (apply f args))
    (catch Exception e (warn e "Exception thrown while reading transaction."))))

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
  [ref-list]
  (apply clj-serialize (map deref ref-list)))

(defn snapshot-filename [id dir]
  (str dir "/" id ".snapshot"))

(defn snapshot-writer [ref-list dir input]
  (let [out (chan)]
    (go-loop [txn (<! input)]
      (>! out txn)
      (<! snapshot-await)
      (alt!
        snapshot-trigger (write-snapshot (create-snapshot ref-list) (snapshot-filename (txn :id) dir))
        :default :noop)
      (recur (<! input)))
    out))

(defn read-snapshot [dir num]
  (deserialize (slurp (str dir "/" num ".snapshot"))))

(defn latest-snapshot-id [dir]
  (apply max (seq (file-numbers dir #"(\d+)\.snapshot\z"))))

(defn apply-snapshot [snapshot ref-list]
  (map #(dosync (ref-set %1 %2)) ref-list snapshot))

(defn journal-to-start [snapshot-num journal-nums]
  (last (filter #(<= % snapshot-num) journal-nums)))

(defn init-db
  "Initialise the persistence base, reading and executing all persisted transactions."
  [& {:keys [data-dirname ref-list batch-size]
      :or {data-dirname "base"
           ref-list nil
           batch-size 1000}}]
  (let [data-dir (File. data-dirname)]
    (if (.exists data-dir)
      (when-not
        (.isDirectory data-dir)
        (throw (RuntimeException. (str "\"" data-dir "\" must be a directory"))))
      (when-not
        (.mkdir data-dir)
        (throw (RuntimeException. (str "Can't create database directory \"" data-dir "\"")))))
    (reset! txn-counter 0M)
    ;Read snapshot TODO
    ;Read journal
    (doseq [n (sort (file-numbers data-dir #"(\d+)\.journal\z"))]
      (read-journal (journal-file-name data-dir n)))
    ;Start writer
    (let [txn-input (tap input-mult (chan))]
      (if (nil? ref-list)
        (txn-executor txn-input)
        (txn-executor (snapshot-writer ref-list data-dir txn-input))))
    (tap input-mult (txn-journaller data-dir batch-size)))) ; taps input transactions into the writer

(defn apply-transaction
  "Persist and apply a transaction, returning a channel that will contain the result. "
  [txn-fn & args]
  (assert (fn? txn-fn) "Transaction to apply must be a fn.")
  (let [c (chan)]
    (put! input-ch {:fn txn-fn :args args :ret c})
    c))

(defn take-snapshot "Pauses execution and takes a snapshot" []
  (put! snapshot-trigger :go))

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
  (init-db :ref-list [msgs] :batch-size 4)
  (dotimes [n 10]
    (<!! (apply-transaction write-msg "Bob" n (now-str))))
  (take-snapshot)
  (dotimes [n 10]
    (thread (<!! (apply-transaction write-msg "Bob" n (now-str)))))
  (Thread/sleep 500)
)