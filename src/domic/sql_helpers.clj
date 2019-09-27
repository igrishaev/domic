(ns domic.sql-helpers
  (:require
   [domic.util :refer [join]]
   [honeysql.core :as sql]))


(defn ->cast
  [field type]
  (if (= type :text)
    field
    (sql/call :cast field (sql/inline type))))


(defn lookup->sql
  [a v type]
  {:select [:e]
   :from [:datoms4]
   :where [:and
           [:= :a a]
           [:= (->cast :v type) v]]
   :limit (sql/inline 1)})


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
  (alias-fields coll [field]))