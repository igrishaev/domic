(ns domic.query-params
  (:require
   [domic.util :refer [extend-print]]
   [honeysql.core :as sql]))


(defprotocol IQueryParams

  (add-alias [this value])

  (add-param [this field value])

  (get-params [this]))


(defn ->cast-timestamp
  [value]
  (sql/call :cast value (sql/inline "timestamp with time zone")))


(defrecord QueryParams
    [params]

    clojure.lang.IDeref

    (deref [this]
      @params)

    IQueryParams

    (add-alias [this value]
      (let [alias (gensym "$")
            param (sql/param alias)
            param (if (inst? value)
                    (->cast-timestamp param)
                    param)]
        (add-param this alias value)
        param))

    ;; todo: drop add-param method
    (add-param [this field value]
      (swap! params assoc field value))

    (get-params [this]
      @params))


(defn params []
  (->QueryParams (atom {})))


(def params? (partial instance? QueryParams))


(extend-print QueryParams)
