(ns domic.pull2
  (:require
   [domic.attr-manager :as am]
   [domic.engine :as en]
   [domic.sql-helpers :refer [adder]]

   [honeysql.core :as sql]
   [honeysql.helpers :as h])
  (:import
   java.sql.ResultSet))


(defn- rs->datom
  [{:as scope :keys [am]}
   ^ResultSet rs]
  (let [attr (keyword (.getString rs 3))
        attr-type (am/get-db-type am attr)]
    {:id (.getLong rs 1)
     :e  (.getLong rs 2)
     :a  attr
     :v  (am/rs->clj attr-type rs 4)
     :t  (.getLong rs 5)}))


(defn- rs->datoms
  [scope]
  (fn [^ResultSet rs]
    (let [result* (transient [])]
      (while (.next rs)
        (conj! result* (rs->datom scope rs)))
      (persistent! result*))))


(defn pull*
  [{:as scope :keys [en]}
   & [elist alist]]

  (let [params* (transient {})
        params*add (adder params*)

        sql (sql/build :select :* :from :datoms4)

        e-params (mapv params*add elist)
        a-params (mapv params*add alist)

        sql
        (cond-> sql

          elist
          (h/merge-where [:in :e e-params])

          alist
          (h/merge-where [:in :a a-params]))

        query (sql/format sql (persistent! params*))]

    (en/query-rs en query (rs->datoms scope))))

#_
(do

  (def _attrs
    [{:db/ident       :artist/name
      :db/valueType   :db.type/string
      :db/cardinality :db.cardinality/one}

     {:db/ident       :release/artist
      :db/valueType   :db.type/ref
      :db/cardinality :db.cardinality/one
      :db/isComponent true}

     {:db/ident       :release/year
      :db/valueType   :db.type/integer
      :db/cardinality :db.cardinality/many}

     {:db/ident       :release/tag
      :db/valueType   :db.type/string
      :db/cardinality :db.cardinality/many}])

  (def _db
    {:dbtype "postgresql"
     :dbname "test"
     :host "127.0.0.1"
     :user "ivan"
     :password "ivan"})

  (def _scope
    {:am (am/manager _attrs)
     :en (en/engine _db)}))
