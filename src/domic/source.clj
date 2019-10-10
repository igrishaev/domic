(ns domic.source)


(defprotocol ISource

  (get-alias [this])

  (get-fields [this])

  (get-data [this]))


(defrecord Table
    [-alias]

  ISource

  (get-alias [this]
    -alias)

  (get-fields [this]
    [:e :a :v :t])

  (get-data [this]))


(defrecord Dataset
    [-alias -fields]

  ISource

  (get-alias [this]
    -alias)

  (get-fields [this]
    -fields))


(defn table [-alias]
  (->Table -alias))


(def table? (partial instance? Table))


(defn dataset [-alias -fields]
  (->Dataset -alias -fields))


(def dataset? (partial instance? Dataset))
