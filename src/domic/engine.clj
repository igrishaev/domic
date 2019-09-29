(ns domic.engine
  (:refer-clojure :exclude [update])
  (:require
   [clojure.java.jdbc :as jdbc]
   [domic.util :refer [kw->str]]
   [honeysql.core :as sql])
  (:import
   [clojure.lang Keyword Symbol]
   org.postgresql.util.PGobject
   org.postgresql.jdbc.PgArray))


(defmacro with-tx
  [binding & body]
  (let [[en-tx en & tx-opt] binding]
    `(let [db-spec# (:db-spec ~en)]
       (jdbc/with-db-transaction
         [tx-spec# db-spec# ~@tx-opt]
         (let [~en-tx (assoc ~en :db-spec tx-spec#)]
           ~@body)))))


(defprotocol IEngine

  (execute
    [this sql-map]
    [this sql-map params])

  (insert-multi
    [this table rows])

  (query-rs [this query rs-fn])

  (query
    [this query]
    [this query opt]))


(defrecord Engine
    [db-spec]

  IEngine

  (query-rs [this query rs-fn]
    (jdbc/db-query-with-resultset db-spec query rs-fn))

  (execute [this sql-map]
    (execute this sql-map nil))

  (execute [this sql-map params]
    (jdbc/execute! db-spec
                   (sql/format sql-map params)))


  (query [this query]
    (jdbc/query db-spec query))

  (query [this query opt]
    (jdbc/query db-spec query opt)))


(defn engine
  [db-spec]
  (->Engine db-spec))


(defn ->pg-obj
  [^String type ^String value]
  (doto (PGobject.)
    (.setType type)
    (.setValue value)))


(extend-protocol jdbc/ISQLValue

  Symbol

  (sql-value [val]
    (->pg-obj "text" (str val)))

  Keyword

  (sql-value [val]
    (->pg-obj "text" (kw->str val))))


(extend-protocol jdbc/IResultSetReadColumn

  PgArray
  (result-set-read-column [pgarray metadata index]
    (let [array-type (.getBaseTypeName pgarray)
          array-java (.getArray pgarray)]
      (with-meta
        (set array-java)
        {:sql/array-type array-type}))))
