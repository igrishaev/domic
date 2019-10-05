(ns domic.pull2
  (:require
   [domic.error :refer [error!]]
   [domic.attributes :as at]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.sql-helpers :as h]
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


(defn pull*-idents
  [{:as scope :keys [table
                     en am]}

   & [eids av-pairs]]

  (let [qp (qp/params)
        add-param (partial qp/add-alias qp)
        ors* (transient [])]

    (when (seq eids)
      (conj! ors* [:in :e (mapv add-param eids)]))

    (doseq [[a v] av-pairs]
      (let [db-type (am/db-type am a)]
        (conj! ors*
               [:and
                [:= :a (add-param a)]
                [:= (h/->cast :v db-type) (add-param v)]])))

    (when-let [ors (-> ors* persistent! not-empty)]
      (let [sql (sql/build :select :*
                           :from table
                           :where (into [:or] ors))
            query (sql/format sql @qp)]
        (en/query-rs en query (rs->datoms scope))))))
