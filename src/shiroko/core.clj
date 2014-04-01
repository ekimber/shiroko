(ns ^{:author "Ed Kimber"}
  shiroko.core
  (:import (java.io File Reader FileOutputStream FileInputStream) [java.math BigDecimal])
  (:use clojure.java.io)
  (:use clojure.tools.logging)
  (:require [clojure.string :refer [join split]])
  (:require [clojure.core.async :as async :refer [put! >!! <!! chan go go-loop map< thread close! mult tap untap filter<]])
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

(def input-ch (map< enumerate-txn (chan 5)))
(def input-mult (mult input-ch))
(def exec-ch (tap input-mult (chan)))
(def journal-channel (map< serialize-txn (tap input-mult (chan))))
(def snapshot-await (chan))
(def exec-await (chan))

(defn ^:private txn-executor [ch]
  (go-loop []
    (let [txn (<! ch)]
      (try
        (put! (:ret txn) (dosync (apply (:fn txn) (:args txn))))
        (catch Exception e
          (warn e "Exception thrown while executing transaction.")
          (close! (:ret txn))))
      (>! exec-await (:id txn))
      (<! snapshot-await))
    (recur)))

(defn ^:private write-to-stream [w txn]
  (.write w (str txn "\n"))
  (.flush w))

(defn ^:private txn-writer [filename]
  (thread
    (with-open [w (writer filename)]
      (loop []
        (let [txn (<!! journal-channel)]
          (when txn
            (write-to-stream w txn)
            (recur)))))))

;; Journal reading
(defn ^:private ingest
  "Update the transaction counter and execute a transaction."
  [[f args txn-id]]
  (assert (= (swap! txn-counter inc) txn-id) "Missing transaction?")
  (try
    (dosync (apply f args))
    (catch Exception e (warn e "Exception thrown while reading transaction."))))

(defn ^:private deserialize [txn]
  (map #(s/deserialize % s/clojure-content-type) (split txn #";")))

(defn ^:private read-journal [filename]
  (with-open [rdr (reader filename)]
   (doseq [line (line-seq rdr)]
     (ingest (deserialize line)))))

(defn ^:private txn-file-number [file]
  (second (re-matches #"(\d+)\.journal\z" (.getName file))))

(defn ^:private journal-numbers [basedir]
  (map #(BigDecimal. %) (remove nil? (map txn-file-number (file-seq basedir)))))

(defn ^:private journal-file-name [data-dir txn-number]
  (str data-dir "/" txn-number ".journal"))

;; Snapshot writing
(defn write-snapshot [snapshot fname]
  (with-open [w (writer fname)]
    (.write w snapshot)
    (.flush w)))

(defn create-snapshot
  "Serialize the persistent refs as a snapshot that will be loaded on next start."
  [ref-list]
  (apply clj-serialize (map deref ref-list)))

(defn snapshot-filename [id data-dir]
  (str data-dir "/" id ".snapshot"))

(defn snapshot-writer [ref-list data-dir]
  (println "Taking snapshots")
  (thread
    (loop []
      (let [last-executed-id (<!! exec-await)]
        (if (zero? (rem last-executed-id 10))
          (write-snapshot (create-snapshot ref-list) (snapshot-filename last-executed-id data-dir))))
      (>!! snapshot-await :go)
      (recur))))

;; Snapshot reading
(defn init-db
  "Initialise the persistence base, reading and executing all persisted transactions."
  [& {:keys [data-dirname ref-list]
      :or {data-dirname "base"
           ref-list nil}}]
  (let [data-dir (File. data-dirname)]
    (if (.exists data-dir)
      (when-not
        (.isDirectory data-dir)
        (throw (RuntimeException. (str "\"" data-dir "\" must be a directory"))))
      (when-not
        (.mkdir data-dir)
        (throw (RuntimeException. (str "Can't create database directory \"" data-dir "\"")))))
    ;Read journal
    (doseq [n (sort (journal-numbers data-dir))]
      (read-journal (journal-file-name data-dir n)))
    ;Start writer
    (if ref-list (snapshot-writer ref-list data-dir))
    (txn-writer (journal-file-name data-dir (inc @txn-counter)))
    (txn-executor exec-ch)))

(defn apply-transaction
  "Persist and apply a transaction, returning a channel that will contain the result. "
  [txn-fn & args]
  (assert (fn? txn-fn) "Transaction to apply must be a fn.")
  (let [c (chan)]
    (put! input-ch {:fn txn-fn :args args :ret c})
    c))

;; APP CODE
(def msgs (ref []))

; Current time as string
(def iso-date (f/formatter "hh:mm:ss.SSS"))
(defn now-str [] (f/unparse iso-date (l/local-now)))

(defn write-msg [name msg time]
  (ref-set msgs (conj @msgs [name msg time])))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (init-db :ref-list [msgs])
  (dotimes [n 13]
    (println (<!! (apply-transaction write-msg "Bob" n (now-str)))))
  (Thread/sleep 500)
)