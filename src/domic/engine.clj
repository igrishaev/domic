(ns domic.engine
  (:require
   [clojure.java.jdbc :as jdbc])
  (:import [clojure.lang Keyword Symbol]
           org.postgresql.util.PGobject)
  )


(defprotocol IEngine

  (query [this query]))


(defrecord Engine
    [db-spec]

  IEngine

  (query [this query]
    (jdbc/query db-spec query)))


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


;; (defmulti pgobject->
;;   (fn [^PGobject pgobj]
;;     (.getType pgobj)))

;; (defmethod pgobject-> "keyword"
;;   [^PGobject pgobj]
;;   (-> pgobj .getValue keyword))



(extend-protocol jdbc/IResultSetReadColumn

  PGobject
  (result-set-read-column [pgobj metadata index]
    (case (.getType pgobj)
      ("json" "jsonb")
      (-> (.getValue pgobj)
          (json/parse-string true)))))
