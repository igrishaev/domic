(ns domic.api
  (:require
   [domic.engine :as en]
   [domic.attr-manager :as am]

   [domic.init :as init]
   [domic.transact :as transact]
   [domic.pull :as pull]
   [domic.query :as query]))

;; todo
;; rename table_seq

(defn ->scope
  [db-spec & [{:keys [table prefix]
               :or {table :datoms
                    prefix ""}}]]

  (let [table*    (str prefix (name table))
        table-log (str table* "_log")
        table-seq (str table* "_seq")

        en (en/engine db-spec)
        am (am/manager)]

    {:en en
     :am am
     :table     (keyword table*)
     :table-log (keyword table-log)
     :table-seq (keyword table-seq)}))


(defn init
  [scope]
  (init/init scope))


(defn sync-attrs
  [{:as scope :keys [am]}]
  (let [attrs (pull/-pull-attrs _scope)]
    (am/reset am attrs))
  nil)


(defn pull
  [scope pattern id]
  (pull/pull scope pattern id))


(defn pull-many
  [scope pattern ids]
  (pull/pull-many scope pattern ids))


(defn q
  [scope query-vect & args]
  (apply query/q scope query-vect args))


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

     {:prefix "__test_"}

     ))

  (def _rules
    '
    [[(queen? ?a)
      [?a :artist/name "Queen"]]

     [(release-of-year? [?r] ?y)
      [?r :release/year ?y]]

     ])

  )
