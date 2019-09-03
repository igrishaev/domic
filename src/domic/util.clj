(ns domic.util
  (:require [clojure.string :as str]))


(def join (partial str/join ", "))


(defn kw->str [kw]
  (-> kw str (subs 1)))


(def zip (partial map vector))
