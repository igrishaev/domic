(ns domic.util
  (:require [clojure.string :as str]))


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
        (symbol (str prefix num))))))
