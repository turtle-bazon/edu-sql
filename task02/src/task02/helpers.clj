(ns task02.helpers)

(defn parse-int [int-str]
  (Integer/parseInt int-str))

(defn cur-time 
  []
  (System/currentTimeMillis))

(def truefn (constantly true))

(defn log-t
  [message throwable]
  (.printStackTrace throwable))