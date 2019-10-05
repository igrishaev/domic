(ns domic.pull2
  (:require
   [domic.error :refer [error!]]
   [domic.attributes :as at]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]

   [honeysql.core :as sql])
  (:import
   java.sql.ResultSet))


(defn- rs->datom
  [{:as scope :keys [am]}
   ^ResultSet rs]
  (let [attr (keyword (.getString rs 3))
        attr-type (am/get-type am attr)]
    {:id (.getLong rs 1)
     :e  (.getLong rs 2)
     :a  attr
     :v  (at/rs->clj attr-type rs 4)
     :t  (.getLong rs 5)}))


(defn- rs->datoms
  [scope]
  (fn [^ResultSet rs]
    (let [result* (transient [])]
      (while (.next rs)
        (conj! result* (rs->datom scope rs)))
      (persistent! result*))))


(defn pull*
  [{:as scope :keys [en table]}
   ids & [attrs]]

  (when-not (seq ids)
    (error! "Empty ids in pull!"))

  (let [qp (qp/params)
        add-param (partial qp/add-alias qp)

        ids*   (mapv add-param ids)
        attrs* (mapv add-param attrs)

        sql (sql/build
             :select :*
             :from table
             :where [:and
                     [:in :e ids*]
                     (when attrs [:in :a attrs*])])

        query (sql/format sql @qp)]

    (en/query-rs en query (rs->datoms scope))))
