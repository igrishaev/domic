(ns domic.api
  (:require
   [domic.transact :as transact]
   [domic.engine :as en]
   [domic.init :as init]
   [domic.attr-manager :as am]))


(defn ->scope
  [db-spec attrs & [{:keys [table prefix]
                     :or {table :datoms
                          prefix ""}}]]

  (let [table*    (str prefix (name table))
        table-log (str table* "_log")
        table-trx (str table* "_trx")
        table-seq (str table* "_seq")

        en (en/engine db-spec)
        am (am/manager attrs)]

    {:en en
     :am am
     :table     (keyword table*)
     :table-log (keyword table-log)
     :table-trx (keyword table-trx)
     :table-seq (keyword table-seq)}))


(defn init
  [scope]
  (init/init scope))


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
  [scope tx-data]
  (transact/transact scope tx-data))


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
       :db/cardinality :db.cardinality/many}]

     {:prefix "__test_"}

     )))
