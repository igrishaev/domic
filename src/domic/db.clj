(ns domic.db)


(defrecord DBPG
    [alias fields])


(defn pg
  []
  (->DBPG :datoms4 [:e :a :v :t]))


(def pg? (partial instance? DBPG))


(defrecord DBTable
    [alias fields data])


(defn table
  [alias fields data]
  (->DBTable alias fields data))


(def table? (partial instance? DBTable))
