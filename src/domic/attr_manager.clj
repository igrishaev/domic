(ns domic.attr-manager
  (:require
   [domic.attributes :as at]
   [domic.error :refer [error!]])
  (:import java.sql.ResultSet))


(defn- group-attrs
  [attr-list]
  (reduce
   (fn [result attrs]
     (let [{:db/keys [ident]} attrs]
       (assoc result ident attrs)))
   {}
   attr-list))


(defprotocol IAttrManager

  (component? [this attr])

  (by-wildcard [this attr])

  (is-ref? [this attr])

  (multiple? [this attr])

  (get-type [this attr])

  (get-db-type [this attr]))


(defrecord AttrManager
    [attr-map]

  IAttrManager

  (component? [this attr]
    (some-> attr-map
            (get attr)
            :db/isComponent))

  (by-wildcard [this attr]
    (let [a-ns (namespace attr)
          a-keys (keys attr-map)]
      (for [a-key a-keys
            :when (= (namespace a-key) a-ns)]
        a-key)))

  (is-ref? [this attr]
    (some-> attr-map
            (get attr)
            :db/valueType
            (= :db.type/ref)))

  (multiple? [this attr]
    (some-> attr-map
            (get attr)
            :db/cardinality
            (= :db.cardinality/many)))

  (get-type [this attr]
    (get-in attr-map [attr :db/valueType]))

  (get-db-type [this attr]
    (let [a-type (get-type this attr)]
      (at/->db-type a-type))))


(defn manager
  ([]
   (manager nil))
  ([attr-list]
   (let [attrs* (-> attr-defaults
                    (concat attr-list)
                    group-attrs)]
     (->AttrManager attrs*))))
