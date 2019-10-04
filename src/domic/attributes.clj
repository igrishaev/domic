(ns domic.attributes
  (:require
   [clojure.string :as str]
   [domic.error :refer [error!]])

  (:import
   clojure.lang.Keyword
   java.sql.ResultSet))


(def defaults

  [{:db/ident       :db/ident
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/doc
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}])


(defn rs->clj
  [^Keyword valueType
   ^ResultSet rs
   ^long index]

  (case valueType

    :db.type/string
    (.getString rs index)

    :db.type/integer
    (.getInt rs index)

    :db.type/ref
    {:db/id (.getLong rs index)}

    ;; else
    (error! "Attribute %s is unknown to ResultSet"
            valueType)))


(defn ->db-type
  [valueType]

  (case valueType
    :db.type/string  :text
    :db.type/integer :integer
    :db.type/ref     :integer

    ;; else
    (error! "Cannot coerce attribute %s to the database type"
            valueType)))


(defn attr-wildcard?
  [^Keyword attr]
  (some-> attr name (= "*")))


(defn attr-backref?
  [^Keyword attr]
  (some-> attr name (str/starts-with? "_")))


(defn attr-backref->ref
  [^Keyword attr]
  (let [a-ns (namespace attr)
        a-name (name attr)]
    (keyword a-ns (subs a-name 1))))
