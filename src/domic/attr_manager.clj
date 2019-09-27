(ns domic.attr-manager
  (:require
   [clojure.string :as str]
   [domic.error :refer [error!]]))

(def attr-defaults

  [{:db/ident       :db/ident
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/doc
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}])


(defn ->back-ref
  [attr]
  (let [ident (:db/ident attr)
        a-ns (namespace ident)
        a-name (name ident)
        ident-rev (keyword a-ns (str "_" a-name))]
    (assoc attr
           :db/doc (format "A backref to %s" ident)
           :db/ident ident-rev
           :db/cardinality :db.cardinality/many)))


(defn backref->ref
  [attr]
  (let [a-ns (namespace attr)
        a-name (name attr)]
    (keyword a-ns (subs a-name 1))))


(defn -ref-attr?
  [attr]
  (some-> attr :db/valueType (= :db.type/ref)))


(defn -backref?
  [attr]
  (some-> attr name (str/starts-with? "_")))


(defn build-back-refs
  [attr-list]
  (->> attr-list
       (filter -ref-attr?)
       (map ->back-ref)))


(defn group-attrs
  [attr-list]
  (reduce
   (fn [result attrs]
     (let [{:db/keys [ident]} attrs]
       (assoc result ident attrs)))
   {}
   attr-list))


(def pg-mapping
  {:db.type/string  :text
   :db.type/ref     :integer
   :db.type/integer :integer})


(defprotocol IAttrManager

  (is-ref? [this attr])

  (multiple? [this attr])

  (get-db-type [this attr])

  (get-pg-type [this attr]))


(defrecord AttrManager
    [attr-map]

  IAttrManager

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

  (get-db-type [this attr]
    (get-in attr-map [attr :db/valueType]))

  (get-pg-type [this attr]
    (let [db-type (get-db-type this attr)]
      (or (get pg-mapping db-type)
          (error! "Unknown type: %s" attr)))))


(defn manager
  ([]
   (manager nil))
  ([attr-list]
   (let [attrs* (-> attr-defaults
                    (concat attr-list)
                    group-attrs)]
     (->AttrManager attrs*))))
