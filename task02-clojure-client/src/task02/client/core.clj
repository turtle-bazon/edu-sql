(ns task02.client.core
  (:require 
    [clojure.java.io :as io]
    [clojure.string :as str])
  (:import
    [java.net Socket]))

(def execution-time (atom 0))

(def execution-counts (atom 0))

(def orig-out *out*)

(defn println-console
  [& messages]
  (binding [*out* orig-out]
    (apply println messages)))

(defn skip-hello
  []
  (read-line))

(defn print-result
  [resultset]
  (let [columns (->> resultset
                  (mapcat keys)
                  distinct)]
    (println-console (str/join "\t" columns))
    (doseq [record resultset]
      (println-console (str/join "\t" (map (partial get record) columns))))))

(defn print-error
  [query error]
  (throw (IllegalStateException. (str "Error received: " error ", for query: " query))))

(defn execute-query
  [query]
  (println query)
  (let [{:keys [status resultset error]} (read-string (read-line))]
    (if (= :ok status)
      (print-result resultset)
      (print-error query error))))

(defn count-query
  [query]
  (println query)
  (let [{:keys [status]} (read-string (read-line))]
    (if (= :ok status)
      true
      false)))

(defn make-disposable-connection
  [^String host port qfn ^String query]
  (let [start-time (System/currentTimeMillis)]
    (with-open [socket (Socket. host port)]
      (binding [*in* (io/reader (.getInputStream socket))
                *out* (io/writer (.getOutputStream socket))]
        (skip-hello)
        (when (qfn query)
          (let [end-time (System/currentTimeMillis)]
            (swap! execution-counts inc)
            (swap! execution-time + (- end-time start-time))))))))

;; (measure-server "localhost" 8778 "select student" 10 1000)
(defn measure-server
  [host port query paralled-threads repeats]
  (reset! execution-counts 0)
  (reset! execution-time 0)
  (doall
    (pmap
      (fn [_]
        (doseq [_ (range repeats)]
          (try
            (make-disposable-connection host port count-query query)
            (catch Throwable t
              nil))))
      (range paralled-threads)))
  {:execution-time @execution-time
   :successfull-executions @execution-counts
   :total-executions (* paralled-threads repeats)
   :output (format "%s query/sec, %.2f%% success"
             (if (= @execution-time 0)
               "inf"
               (format "%.2f" (-> @execution-counts
                                (/ (/ @execution-time 1000))
                                double)))
             (-> @execution-counts
               (/ (* paralled-threads repeats))
               double
               (* 100)))})
