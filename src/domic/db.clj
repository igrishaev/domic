(ns domic.db
  (:require
   [clojure.java.jdbc :as jdbc]

   [domic.error :refer [error!]]
   [domic.var-manager :as vm]
   [domic.query-builder :as qb]
   [domic.attr-manager :as am]
   [domic.util :refer [join kw->str zip]]

   [honeysql.core :as sql]))


(defprotocol IDB

  (add-pattern [this expression vm qb am])

  (set-fields [this fields])

  (db-init [this qb])

  (db-type [this]))


(def db? (partial satisfies? IDB))

(defrecord DBPG
    [as fields]

    IDB

    (db-type [this] :pg)

    (db-init [this qb])

    (add-pattern [this expression vm qb am]

      (let [{:keys [elems]} expression
            [_ A] elems
            layer (gensym "DATOMS")

            attr
            (when A
              (let [[tag a] A]
                (case tag
                  :cst
                  (let [[tag a] a]
                    (case tag
                      :kw a)))))

            type-pg
            (when attr
              (am/get-pg-type am attr))]

        (qb/add-from qb [as layer])

        (doseq [[elem field] (zip elems fields)]

          (let [[tag elem] elem

                cast
                (when (and (= field 'v)
                           (some? type-pg)
                           (not= type-pg "text"))
                  (format "::%s" type-pg))

                fq-field
                (sql/raw
                 (format "%s.%s%s" layer field (or cast "")))]

            (case tag

              :cst
              (let [[tag v] elem]
                (let [where [:= fq-field v]]
                  (qb/add-where qb where)))

              :var
              (if (vm/bound? vm elem)
                (let [where [:= fq-field (vm/get-val vm elem)]]
                  (qb/add-where qb where))
                (vm/bind! vm elem fq-field :where elems nil))))))))


(defn db-pg []
  (->DBPG :datoms '[e a v t]))


(def db-pg? (partial instance? DBPG))


(defrecord DBTable
    [data as fields]

  IDB

  (db-type [this] :table)

  (db-init [this qb]
    (let [with [[as {:columns fields}] {:values data}]]
      (qb/add-with qb with)))

  (add-pattern [this expression vm qb am]

    (let [{:keys [elems]} expression
          layer (gensym "TABLE")]

      (qb/add-from qb [as layer])

      (doseq [[elem field] (zip elems fields)]

        (let [[tag elem] elem
              fq-field (sql/raw (format "%s.%s" layer field))]

          (case tag

            :cst
            (let [[tag v] elem]
              (let [where [:= fq-field v]]
                (qb/add-where qb where)))

            :var
            (if (vm/bound? vm elem)
              (let [where [:= fq-field (vm/get-val vm elem)]]
                (qb/add-where qb where))
              (vm/bind! vm elem fq-field :where elems nil))))))))


(defn ->db-table
  [data]
  (let [[row] data
        as (gensym "TABLE")
        fields (for [_ row] (gensym "FIELD"))]
    (->DBTable data as fields)))


(defn ->db
  [param]
  (cond
    (db? param) param
    :else (->db-table param)))
