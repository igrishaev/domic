(ns domic.db)


(defrecord DBPG
    [alias fields])


(defn db-pg
  []
  (->DBPG :datoms3 '[e a v t]))


(def db-pg? (partial instance? DBPG))


(defrecord DBTable
    [data alias fields])


(defn db-table
  [data alias fields]
  (->DBTable data alias fields))


(def db-table? (partial instance? DBTable))
