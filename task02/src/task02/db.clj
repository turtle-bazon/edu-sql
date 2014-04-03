(ns task02.db
  "Namespace for database-related data & functions..."
  (:require [clojure.data.csv :as csv]
            [clojure.string :as str])
  (:use task02.helpers))

;; helper functions

(defn table-keys [tbl]
  (mapv keyword (first tbl)))

(defn key-value-pairs [tbl-keys tbl-record]
  (interleave tbl-keys tbl-record))

(def data-record zipmap)

(defn data-table [tbl]
  (map (partial data-record (table-keys tbl)) (next tbl)))

(defn str-field-to-int [field rec]
  (update-in rec [field] parse-int))

;; Место для хранения данных - используйте atom/ref/agent/...
;; ref с той целью, чтобы была соблюдена транзакционность 
;; создания и удаления таблиц. Чтобы между этими событиями
;; вся база данных оставалась консистентной. То есть транзакция
;; по созданию таблицы и её заполнению. Чтобы rollback можно
;; было делать
(def tablespaces (ref {}))

;; функция должна вернуть мутабельный объект используя его имя
(defn get-table [^String tb-name]
  (let [lname (str/lower-case tb-name)]
    (get @tablespaces lname)))

;;; Данная функция загружает начальные данные из файлов .csv
;;; и сохраняет их в изменяемых переменных student, subject, student-subject
(defn load-initial-data []
  ;;; :implement-me может быть необходимо добавить что-то еще
  (dosync
    (ref-set tablespaces
      {"student" (ref (->> (data-table (csv/read-csv (slurp "student.csv")))
                        (map #(str-field-to-int :id %))
                        (map #(str-field-to-int :year %))))
       "subject" (ref (->> (data-table (csv/read-csv (slurp "subject.csv")))
                        (map #(str-field-to-int :id %))))
       "student-subject" (ref (->> (data-table (csv/read-csv (slurp "student_subject.csv")))
                                (map #(str-field-to-int :subject_id %))
                                (map #(str-field-to-int :student_id %))))})
    :ok))

;; select-related functions...
(defn where* [data condition-func]
  (if condition-func
    (filter condition-func data)
    data))

(defn limit* [data lim]
  (if lim
    (take lim data)
    data))

(defn order-by* [data column]
  (if column
    (sort-by column data)
    data))

(defn join* [data1 column1 data2 column2]
  (for [left data1
        right data2
        :when (= (column1 left) (column2 right))]
    (merge right left)))

;; Внимание! в отличии от task01, в этом задании 2-я таблица передается в виде строки с именем таблицы!
(defn perform-joins [data joins]
  (if (empty? joins)
    data
    (let [[col1 data2 col2] (first joins)]
      (recur (join* data col1 @(get-table data2) col2)
             (next joins)))))

;; Данная функция аналогична функции в первом задании за исключением параметра :joins как описано выше.
;; примеры использования с измененым :joins:
;;   (select student-subject :joins [[:student_id "student" :id] [:subject_id "subject" :id]])
;;   (select student-subject :limit 2 :joins [[:student_id "student" :id] [:subject_id "subject" :id]])
(defn select [data & {:keys [where limit order-by joins]}]
  {:status :ok
   :resultset (-> @data
                (perform-joins joins)
                (where* where)
                (order-by* order-by)
                (limit* limit))})

;; insert/update/delete...

;; Данная функция должна удалить все записи соответствующие указанному условию
;; :where. Если :where не указан, то удаляются все данные.
;;
;; Аргумент data передается как мутабельный объект (использование зависит от выбора ref/atom/agent/...)
;;
;; Примеры использования
;;   (delete student) -> []
;;   (delete student :where #(= (:id %) 1)) -> все кроме первой записи
(defn delete [data & {:keys [where]}]
  (dosync
    (alter data (comp vec (partial remove (or where truefn))))
    {:status :ok
     :resultset nil}))

;; Данная функция должна обновить данные в строках соответствующих указанному предикату
;; (или во всей таблице).
;;
;; - Аргумент data передается как мутабельный объект (использование зависит от выбора ref/atom/agent/...)
;; - Новые данные передаются в виде map который будет объединен с данными в таблице.
;;
;; Примеры использования:
;;   (update student {:id 5})
;;   (update student {:id 6} :where #(= (:year %) 1996))
(defn update [data upd-map & {:keys [where]}]
  (let [where-function (or where truefn)]
    (dosync
      (alter data (partial mapv (fn [vrecord]
                                  (if (where-function vrecord)
                                    (merge vrecord upd-map)
                                    vrecord))))
      {:status :ok
       :resultset nil})))

;; Вставляет новую строку в указанную таблицу
;;
;; - Аргумент data передается как мутабельный объект (использование зависит от выбора ref/atom/agent/...)
;; - Новые данные передаются в виде map
;;
;; Примеры использования:
;;   (insert student {:id 10 :year 2000 :surname "test"})
(defn insert [data new-entry]
  (dosync
    (alter data conj new-entry)
    {:status :ok
     :resultset nil}))

(defn create-table
  [table-name]
  (dosync
    (when (contains? @tablespaces table-name)
      (throw (IllegalAccessException. "Table already exists")))
    (alter tablespaces assoc table-name (ref []))
    {:status :ok
     :resultset nil}))

(defn drop-table
  [table-name]
  (dosync
    (alter tablespaces dissoc table-name)
    {:status :ok
     :resultset nil}))
