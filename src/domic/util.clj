(ns domic.util
  (:require [clojure.string :as str])
  (:import clojure.lang.IPersistentMap))


(def join (partial str/join ", "))


(defn kw->str [kw]
  (-> kw str (subs 1)))


(def zip (partial map vector))


(defn sym-generator
  []
  (let [state (atom {})
        inc* (fnil inc 0)]
    (fn [& [prefix]]
      (let [prefix (or prefix "_")
            state* (swap! state update prefix inc*)
            num (get state* prefix)]
        (keyword (str prefix num))))))


(defn drop-nils
  [map]
  (let [map* (transient {})]
    (doseq [[k v] map]
      (when (some? v)
        (assoc! map* k v)))
    (persistent! map*)))


(defmacro extend-print
  [MapClass]
  `(defmethod print-method ~MapClass
     [~'o ~'w]
     ((get-method print-method IPersistentMap) ~'o ~'w)))
