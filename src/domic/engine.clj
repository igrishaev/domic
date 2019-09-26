(ns domic.engine
  (:require
   [clojure.java.jdbc :as jdbc])
  (:import
   [clojure.lang Keyword Symbol]
   org.postgresql.util.PGobject
   org.postgresql.jdbc.PgArray))


(defprotocol IEngine

  (query
    [this query]
    [this query opt]))


(defrecord Engine
    [db-spec]

  IEngine

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
    (->pg-obj "text" (-> val str)))

  Keyword

  (sql-value [val]
    (->pg-obj "text" (-> val str (subs 1)))))


(extend-protocol jdbc/IResultSetReadColumn

  PgArray
  (result-set-read-column [pgarray metadata index]
    (let [array-type (.getBaseTypeName pgarray)
          array-java (.getArray pgarray)]
      (with-meta
        (set array-java)
        {:sql/array-type array-type}))))
