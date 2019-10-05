(ns domic.sql-helpers
  (:require
   [domic.util :refer [join]]
   [honeysql.core :as sql]))

;; todo
;; drop adder

(defn ->cast
  [field type]
  (if (= type :text)
    field
    (sql/call :cast field (sql/inline type))))


(defn lookup?
  [node]
  (and (vector? node)
       (keyword? (first node))))


(defn as-fields
  [coll fields]
  (sql/inline
   (format "%s (%s)"
           (name coll)
           (join (map name fields)))))


(defn as-field
  [coll field]
  (as-fields coll [field]))


(defn adder
  [tr-map]
  (fn [value]
    (let [alias (gensym)
          param (sql/param alias)]
      (assoc! tr-map alias value)
      param)))
