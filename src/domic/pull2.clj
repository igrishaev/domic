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


(defn ->param []
  (let [alias (gensym)]
    [alias (sql/param alias)]))


(defn pull*
  [{:as scope :keys [en]}
   & [elist alist rlist]]

  (let [params* (transient {})

        e-params* (transient [])
        a-params* (transient [])
        r-params* (transient [])

        sql (sql/build :select :* :from :datoms4)

        _
        (doseq [e elist]
          (let [[alias param] (->param)]
            (conj! e-params* param)
            (assoc! params* alias e)))

        _
        (doseq [a alist]
          (let [[alias param] (->param)]
            (conj! a-params* param)
            (assoc! params* alias a)))

        _
        (doseq [r rlist]
          (let [[alias param] (->param)]
            (conj! r-params* param)
            (assoc! params* alias r)))

        e-params (-> e-params* persistent! not-empty)
        a-params (-> a-params* persistent! not-empty)
        r-params (-> r-params* persistent! not-empty)

        sql
        (cond-> sql

          e-params
          (h/merge-where [:in :e e-params])

          a-params
          (h/merge-where [:in :a a-params])

          r-params
          (h/merge-where [:in
                          (sql/call :cast :v :integer)
                          r-params]))

        params (persistent! params*)
        query (sql/format sql params)]

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
