(ns domic.attr-manager
  (:require
   [domic.attributes :as at]
   [domic.util :refer [extend-print]]
   [domic.error :refer [error!]]))


(defn- group-attrs
  [attr-list]
  (reduce
   (fn [result attrs]
     (let [{:db/keys [ident]} attrs]
       (assoc result ident attrs)))
   {}
   attr-list))


(defprotocol IAttrManager

  (known? [this attr])

  (index? [this attr])

  (component? [this attr])

  (by-wildcard [this attr])

  (ref? [this attr])

  (unique? [this attr])

  (multiple? [this attr])

  (get-type [this attr])

  (db-type [this attr]))


(defrecord AttrManager
    [attr-map]

  clojure.lang.IDeref

  (deref [this] (keys attr-map))

  IAttrManager

  (known? [this attr]
    (contains? attr-map attr))

  (index? [this attr]
    (get-in attr-map [attr :db/index]))

  (unique? [this attr]
    (get-in attr-map [attr :db/unique]))

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

  (ref? [this attr]
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

  (db-type [this attr]
    (let [a-type (get-type this attr)]
      (at/->db-type a-type))))


(defn manager
  ([]
   (manager nil))
  ([attr-list]
   (let [attrs* (-> at/defaults
                    (concat attr-list)
                    group-attrs)]
     (->AttrManager attrs*))))


(extend-print AttrManager)
