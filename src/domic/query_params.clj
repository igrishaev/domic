(ns domic.query-params)


(defprotocol IQueryParams

  (add-param [this field value])

  (get-params [this]))


(defrecord QueryParams
    [params]

  IQueryParams

  (add-param [this field value]
    (swap! params assoc field value))

  (get-params [this]
    @params))


(defn params []
  (->QueryParams (atom {})))


(def params? (partial instance? QueryParams))
