(ns domic.api
  (:require
   [domic.engine :as en]
   [domic.attr-manager :as am]))


(defn ->scope
  [db-spec attrs]
  (let [en (en/engine db-spec)
        am (am/manager attrs)]
    {:en en :am am}))


(defn pull
  [scope pattern id])


(defn pull-many
  [scope pattern ids])


(defn q
  [])


(defn entity
  [])


(defn touch
  [])


(defn transact
  [])


#_
(do

  (def _scope

    (->scope

     {:dbtype "postgresql"
      :dbname "test"
      :host "127.0.0.1"
      :user "ivan"
      :password "ivan"
      :assumeMinServerVersion "10"}

     [{:db/ident       :artist/name
       :db/valueType   :db.type/string
       :db/cardinality :db.cardinality/one}

      {:db/ident       :release/artist
       :db/valueType   :db.type/ref
       :db/cardinality :db.cardinality/many
       :db/isComponent true}

      {:db/ident       :release/year
       :db/valueType   :db.type/integer
       :db/cardinality :db.cardinality/one}

      {:db/ident       :release/tag
       :db/valueType   :db.type/string
       :db/cardinality :db.cardinality/many}])))
