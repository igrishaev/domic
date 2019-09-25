(ns domic.engine
  (:require
   [clojure.java.jdbc :as jdbc])
  (:import [clojure.lang Keyword Symbol]
           org.postgresql.util.PGobject
           org.postgresql.jdbc.PgArray

           )
  )


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


(defn ->pgobject
  [^Keyword type ^String value]
  (doto (PGobject.)
    (.setType type)
    (.setValue value)))




(extend-protocol jdbc/ISQLValue

  Symbol

  (sql-value [val]
    (->pgobject "text" (str val)))

  Keyword

  (sql-value [val]
    (->pgobject "text" (subs (str val) 1))))


(extend-protocol jdbc/IResultSetReadColumn

  PgArray
  (result-set-read-column [pgarray metadata index]
    (let [;; array-type (.getBaseTypeName pgarray)
          array-java (.getArray pgarray)]

      (set array-java)

      #_
      (with-meta
        (set array-java)
        {:sql/array-type array-type}))))


;; (defmulti pgobject->
;;   (fn [^PGobject pgobj]
;;     (.getType pgobj)))

;; (defmethod pgobject-> "keyword"
;;   [^PGobject pgobj]
;;   (-> pgobj .getValue keyword))



#_
(extend-protocol jdbc/IResultSetReadColumn

  PGobject
  (result-set-read-column [pgobj metadata index]
    (case (.getType pgobj)
      ("json" "jsonb")
      (-> (.getValue pgobj)
          (json/parse-string true)))))


#_
(defn ->pg
  [value]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (json/generate-string value))))
