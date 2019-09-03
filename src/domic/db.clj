(ns domic.db
  (:require
   [clojure.java.jdbc :as jdbc]

   [domic.error :refer [error!]]
   [domic.var-manager :as vm]
   [domic.query-builder :as qb]
   [domic.attr-manager :as am]
   [domic.util :refer [join kw->str]]

   [honeysql.core :as sql]))


(defprotocol IDB

  (add-pattern [this expression vm qb am])

  (db-init [this qb])

  (db-type [this]))


(def db? (partial satisfies? IDB))


(defrecord DBPG
    []

  IDB

  (db-type [this] :pg)

  (db-init [this qb]
    nil)

  (add-pattern [this expression vm qb am]
    (let [{:keys [src-var
                  elems]} expression
          [e a v t] elems

          prefix (str (gensym "d"))

          attr
          (let [[tag a] a]
            (case tag
              :cst
              (let [[tag a] a]
                (case tag
                  :kw a))))

          pg-type (am/get-pg-type am attr)]

      (qb/add-from qb [:datoms (keyword prefix)])

      ;; E
      (let [[tag e] e]
        (case tag
          :var
          (let [sql (keyword (format "%s.e" prefix))]
            (if (vm/bound? vm e)
              (let [where [:= sql (vm/get-val vm e)]]
                (qb/add-where qb where))
              (vm/bind! vm e sql :where elems :integer)))))

      ;; A
      (let [where [:= (sql/raw (format "%s.a" prefix)) (kw->str attr)]]
        (qb/add-where qb where))

      ;; V
      (let [[tag v] v]

        (let [sql (sql/raw (format "%s.v::%s" prefix pg-type))]

          (case tag
            :cst
            (let [[tag v] v]
              (let [where [:= sql v]]
                (qb/add-where qb where)))

            :var
            (if (vm/bound? vm v)
              (let [where [:= sql (vm/get-val vm v)]]
                (qb/add-where qb where))
              (vm/bind! vm v sql :where elems pg-type))))))))


(defn db-pg []
  (->DBPG))


(def db-pg? (partial instance? DBPG))


(defrecord DBTable
    [table]

  IDB

  (db-type [this] :table)

  (db-init [this qb]
    (let [[row] table
          as (gensym "coll")
          fields (for [_ row] (gensym "var"))
          alias (sql/raw (format "%s (%s)" as (join fields)))
          from [{:values table} alias]]
      (qb/add-from qb from)))

  (add-pattern [this expression vm qb am]
    (error! "not implemented")))


(defn ->db-table
  [table]
  (->DBTable table))


(defn ->db
  [param]
  (cond
    (db? param) param
    :else (->db-table param)))
