(ns shiroko.core
  (:gen-class))
(require '[clojure.core.async :as async :refer [put! >!! <!! chan go go-loop map< thread mult tap]])
(require '[clj-time.core :as t])
(require '[clj-time.format :as f])
(require '[clj-time.local :as l])
(require '[clojurewerkz.serialism.core :as s])

(def transaction-counter (atom 0M))

(defn serialized-transaction
  [txn-forms]
  (s/serialize )
;  (str "(" txn-fn " " (clojure.string/join " " (map pr-str txn-args)) ")" )
  )

(def input-channel (chan))
(def input-mult (mult input-channel))
(def exec-channel (tap input-mult (chan)))
(def journal-channel (map< #(serialized-transaction (:fn-name %) (:fn-args %))  (tap input-mult (chan))))
;(def journal-channel (map< #(serialized-transaction (:fn-name %) (:fn-args %)) input-channel))

(defn write-txn [txn-id txn-str]
   (Thread/sleep 5)
;  (println "foo")
  (println (str txn-str "; " txn-id " [" (System/currentTimeMillis) "]"))
)

(defn txn-writer [ch]
  (println "Receiving...")
  (go-loop []
    (write-txn @transaction-counter (<!! ch))
    (swap! transaction-counter inc)
    (recur)))
;
(defn txn-executor [ch]
  (go-loop []
    (let [v (<!! ch)]
      (println (:fn-args v))
      (apply (:fn v) (:fn-args v)))
    (recur)))

(txn-writer journal-channel)
(txn-executor exec-channel)
(defn sync-exec [f & args]
  (dosync (f args)))

(defmacro apply-transaction
  "Apply transaction to the root object and write it to disk."
  [transaction-fn & transaction-fn-arg]
  `(put! input-channel {:fn ~transaction-fn :fn-name '~transaction-fn :fn-args [~@transaction-fn-arg]}))
;     (dosync (~transaction-fn ~@transaction-fn-arg))))
;     (fn [_]
;       (dosync (~transaction-fn ~@transaction-fn-arg)))))
;(~transaction-fn ~@transaction-fn-arg)
;; Current time as string
(def iso-date (f/formatter "hh:mm:ss.SSS"))
(defn now-str [] (f/unparse iso-date (l/local-now)))

(defn write [name msg time]
  (println (str "recv -> " name ": " msg " " time "; "))
  (str name " " msg)
  ;  (ref-set msgs (conj @msgs [msg time]))
  )


(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (dotimes [n 10]
    (go (Thread/sleep (rand 100))
        (apply-transaction shiroko.core/write "Bob" n (now-str))
    )
  )
  (Thread/sleep 1000)
)
