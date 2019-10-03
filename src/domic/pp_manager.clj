(ns domic.pp-manager)


(defprotocol IPPManager

  (add-pull [this index pattern]))


(defrecord PPManager
    [index*]

  clojure.lang.IDeref

  (deref [this]
    @index*)

  IPPManager

  (add-pull [this idx pattern]
    (swap! index* assoc idx
           {:type :pull :pattern pattern})))


(defn manager []
  (->PPManager (atom {})))
