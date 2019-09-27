(ns domic.sql-helpers
  (:require
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
   :limit 1})


(defn lookup?
  [node]
  (and (vector? node)
       (keyword? (first node))))
