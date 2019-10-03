(ns domic.query-params
  (:require
   [honeysql.core :as sql]))


(defprotocol IQueryParams

  (add-alias [this value])

  (add-param [this field value])

  (get-params [this]))


(defrecord QueryParams
    [params]

  clojure.lang.IDeref

  (deref [this]
    @params)

  IQueryParams

  (add-alias [this value]
    (let [alias (gensym "param")
          param (sql/param alias)]
      (add-param this alias value)
      param))

  (add-param [this field value]
    (swap! params assoc field value))

  (get-params [this]
    @params))


(defn params []
  (->QueryParams (atom {})))


(def params? (partial instance? QueryParams))
