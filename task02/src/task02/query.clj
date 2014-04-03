(ns task02.query
  (:use 
    [clojure.core.match :only (match)]
    [task02 helpers db])
  (:require
    [clojure.string :as str]))

(def sql-keywords
  ["select"
   "delete"
   "insert"
   "update"
   "where"
   "order"
   "by"
   "limit"
   "join"
   "on"])

(defn parse-value
  [value]
  (if (and (= (get value 0) \')
           (= (get value (dec (.length value))) \'))
    (str/replace value "'" "")
    (parse-int value)))

(defn make-where-function [& [column cmpf value]]
  (let [wfunction (case cmpf
                    "=" =
                    "!=" not=
                    "<" <
                    ">" >
                    "<=" <=
                    ">=" >=)]
    (fn [record]
      (wfunction
        ((keyword column) record)
        (parse-value value)))) )

(defn make-select
  [table]
  {:query-function select
   :table-function get-table
   :table table
   :joins []})

(defn make-delete
  [table]
  {:query-function delete
   :table-function get-table
   :table table})

(defn make-update
  [table]
  {:query-function update
   :table-function get-table
   :table table
   :update-map {}})

(defn make-insert
  [table]
  {:query-function insert
   :table-function get-table
   :table table
   :value-map {}})

(defn make-create-table
  [table]
  {:query-function create-table
   :table-function identity
   :table table})

(defn make-drop-table
  [table]
  {:query-function drop-table
   :table-function identity
   :table table})

(defn append-where
  [query c op v]
  (assoc-in query [:where] (make-where-function c op v)))

(defn append-ordery-by
  [query c]
  (assoc-in query [:order-by] (keyword c)))

(defn append-limit
  [query n]
  (assoc-in query [:limit] (parse-int n)))

(defn append-join
  [query table lcolumn rcolumn]
  (update-in query [:joins] conj [(keyword lcolumn) table (keyword rcolumn)]))

(defn append-update-map
  [query column value]
  (assoc-in query [:update-map (keyword column)] (parse-value value)))

(defn append-value-map
  [query column value]
  (assoc-in query [:value-map (keyword column)] (parse-value value)))

(defn parse-query-seq
  ([query-seq]
    (parse-query-seq (list) query-seq))
  ([query query-seq]
    (if (empty? query-seq)
      query
      (let [[part rest] 
            (match (vec query-seq)
              ["select" table & r] [(make-select table) r]
              ["delete" table & r] [(make-delete table) r]
              ["update" table & r] [(make-update table) r]
              ["insert" table & r] [(make-insert table) r]
              ["create" "table" table & r] [(make-create-table table) r]
              ["drop" "table" table & r]   [(make-drop-table table) r] 
              ["where" c op v & r] [(append-where query c op v) r] 
              ["order" "by" c & r] [(append-ordery-by query c) r]
              ["limit" n & r]      [(append-limit query n) r]
              ["join" table "on" 
               lcolumn "=" rcolumn & r] [(append-join query table lcolumn rcolumn) r]
              ["set" c "=" v & r]  [(append-update-map query c v) r]
              ["val" c "=" v & r]  [(append-value-map query c v) r])]
        (if part
          (parse-query-seq part rest)
          (throw (IllegalArgumentException. (str "Wrong sql-seq: " query-seq))))))))

(defn split-parse
  [^String query-string]
  (map
    (fn [token]
      (let [lcase (str/lower-case token)]
        (if (some #(= % lcase) sql-keywords)
          lcase
          token)))
    (str/split query-string #" ")))

(defn parse-query
  [^String query-string]
  (try
    (let [query (parse-query-seq (split-parse query-string))]
      (apply concat
        (for [k [:query-function :update-map :value-map :where :order-by :limit :joins]
             :let [v (k query)]
             :when (not (or (nil? v)
                            (and (= k :joins) (empty? v))))]
          (cond
            (= k :query-function) [v (:table-function query) (:table query)]
            (= k :update-map) [(:update-map query)]
            (= k :value-map) [(:value-map query)]
            :else [k v]))))
    (catch Throwable e
      nil)))

;; Функция выполняющая парсинг запроса переданного пользователем
;;
;; Синтаксис запроса:
;; SELECT table_name [WHERE column comp-op value] [ORDER BY column] [LIMIT N] [JOIN other_table ON left_column = right_column]
;;
;; - Имена колонок указываются в запросе как обычные имена, а не в виде keywords. В
;;   результате необходимо преобразовать в keywords
;; - Поддерживаемые операторы WHERE: =, !=, <, >, <=, >=
;; - Имя таблицы в JOIN указывается в виде строки - оно будет передано функции get-table для получения объекта
;; - Значение value может быть либо числом, либо строкой в одинарных кавычках ('test').
;;   Необходимо вернуть данные соответствующего типа, удалив одинарные кавычки для строки.
;;
;; - Ключевые слова --> case-insensitive
;;
;; Функция должна вернуть последовательность со следующей структурой:
;;  - имя таблицы в виде строки
;;  - остальные параметры которые будут переданы select
;;
;; Если запрос нельзя распарсить, то вернуть nil

;; Примеры вызова:
;; > (parse-select "select student")
;; ("student")
;; > (parse-select "select student where id = 10")
;; ("student" :where #<function>)
;; > (parse-select "select student where id = 10 limit 2")
;; ("student" :where #<function> :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2")
;; ("student" :where #<function> :order-by :id :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2 join subject on id = sid")
;; ("student" :where #<function> :order-by :id :limit 2 :joins [[:id "subject" :sid]])
;; > (parse-select "werfwefw")
;; nil
(defn parse-select [^String sel-string]
  (doall (drop 2 (parse-query sel-string))))

;; Выполняет запрос переданный в строке.  Бросает исключение если не удалось распарсить запрос
;; Примеры вызова:
;; > (perform-query "select student")
;; ({:id 1, :year 1998, :surname "Ivanov"} {:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "select student order by year")
;; ({:id 3, :year 1996, :surname "Sidorov"} {:id 2, :year 1997, :surname "Petrov"} {:id 1, :year 1998, :surname "Ivanov"})
;; > (perform-query "select student where id > 1")
;; ({:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "not valid")
;; exception...
(defn perform-query [^String query-string]
  (if-let [[query-function table-function & [table-name & args]] (parse-query query-string)]
    (try
      (apply query-function (table-function table-name) args)
      (catch Throwable e
        {:status :error
         :resultset nil
         :error (str e)}))
    (throw (IllegalArgumentException. (str "Can't parse query: " query-string)))))
