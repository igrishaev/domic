(ns domic.engine
  (:require
   [clojure.java.jdbc :as jdbc]
   [domic.util :refer [kw->str]])
  (:import
   [clojure.lang Keyword Symbol]
   org.postgresql.util.PGobject
   org.postgresql.jdbc.PgArray))


(defprotocol IEngine

  (insert-multi
    [this table rows])

  (query
    [this query]
    [this query opt]))


(defrecord Engine
    [db-spec]

  IEngine

  (insert-multi [this table rows]
    (jdbc/insert-multi! db-spec table rows))

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
