(ns domic.runtime
  (:require
   [domic.error :refer [error!]]
   [domic.sql-helpers :refer [->cast]]
   [domic.query-builder :as qb]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]

   [honeysql.core :as sql]))


(defn resolve-lookup
  [{:as scope :keys [table
                     en am]}
   [a v]]

  (let [qb (qb/builder)
        qp (qp/params)

        alias-a :a
        alias-v :v

        param-a (sql/param alias-a)
        param-v (sql/param alias-v)

        db-type (am/db-type am a)]

    (qp/add-param qp alias-a a)
    (qp/add-param qp alias-v v)

    (qb/add-select qb :e)
    (qb/add-from   qb table)
    (qb/add-where  qb [:= :a param-a])
    (qb/add-where  qb [:= (->cast :v db-type) param-v])
    (qb/set-limit  qb 1)

    (-> (->> (qp/get-params qp)
             (qb/format qb)
             (en/query en))
        first :e)))


(defn resolve-lookup!
  [scope lookup]
  (or (resolve-lookup scope lookup)
      (error! "Lookup failed: %s" lookup)))


(defn allocate-db-ids
  [{:as scope :keys [table-seq
                     en]}
   temp-ids]
  (when (seq temp-ids)
    (let [qb (qb/builder)
          nextval (sql/call :nextval (name table-seq))]
      (doseq [temp-id temp-ids]
        (qb/add-select qb [nextval temp-id]))

      (first
       (en/query en (qb/format qb)
                 {:keywordize? false
                  :identifiers identity})))))
