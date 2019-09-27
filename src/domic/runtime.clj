(ns domic.runtime
  (:require
   [domic.error :refer [error!]]
   [domic.query-builder :as qb]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]

   [honeysql.core :as sql]))


(defn resolve-lookup
  [{:as scope :keys [en am]}
   lookup]

  (let [[a v] lookup

        qb (qb/builder)
        qp (qp/params)

        alias-a :a
        alias-v :v

        param-a (sql/param alias-a)
        param-v (sql/param alias-v)

        pg-type (am/get-pg-type am a)]

    (qp/add-param qp alias-a a)
    (qp/add-param qp alias-v v)

    (qb/add-select qb :e)
    (qb/add-from   qb :datoms4)
    (qb/add-where  qb [:= :a param-a])
    (qb/add-where  qb [:= :v param-v])
    (qb/set-limit  qb 1)

    (-> (->> (qp/get-params qp)
             (qb/format qb)
             (en/query en))
        first :e)))

(defn resolve-lookup!
  [scope lookup]
  (or (resolve-lookup scope lookup)
      (error! "Lookup failed: %s" lookup)))
