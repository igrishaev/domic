(ns domic.pull
  (:require
   [clojure.spec.alpha :as s]

   [domic.util :refer [kw->str]]
   [domic.error :refer [error!]]
   [domic.query-builder :as qb]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]

   [honeysql.core :as sql]
   [datomic-spec.core :as ds]))


(defn- qb-add-entities
  [qb e]
  (cond
    (int? e)
    (qb/add-where qb [:= :e e])

    (coll? e)
    (qb/add-where qb [:in :e e])

    :else
    (error! "Wrong entity param: %s" e)))


(defn- resolve-attrs
  [{:as scope :keys [en]}
   e]

  (let [qb (qb/builder)]
    (qb-add-entities qb e)
    (qb/add-select qb :a)
    (qb/add-from qb :datoms4)

    (let [query (qb/format qb)
          result (en/query en query {:as-arrays? true})]

      (->> (rest result)
           (map (comp keyword first))
           set))))


(defn- -pull
  [{:as scope :keys [en am sg]}
   attrs e]

  (let [qb (qb/builder)
        qp (qp/params)
        alias-sub :subquery
        qb-sub (qb/builder)]

    (qb/add-select qb-sub :*)
    (qb/add-from qb-sub :datoms4)
    (qb-add-entities qb e)

    (qb/add-from qb [(qb/->map qb-sub) alias-sub])
    (qb/add-select qb [:e "db/id"])
    (qb/add-group-by qb :e)

    (doseq [attr attrs]

      (let [multiple?  (am/multiple? am attr)
            attr-param (sql/param attr)
            pg-type    (am/get-pg-type am attr)
            agg        (if multiple? :array_agg :max)
            cast       (partial sql/call :cast)

            clause
            (sql/raw
             [(sql/call agg (cast :v (sql/inline pg-type)))
              " filter "
              {:where [:and [:= :a (sql/param attr)]]}])]

        (qp/add-param qp attr attr)
        (qb/add-select qb [clause (kw->str attr)])))

    (let [params (qp/get-params qp)]
      (en/query en (qb/format qb params)))))


(defn- pull-join [p1 p2 attr multiple?]

  (let [grouped (group-by :db/id p2)
        getter (fn [e]
                 (first (get grouped e)))
        updater (if multiple?
                  (fn [es]
                    (map getter es))
                  getter)]

    (for [p p1]
      (update p attr updater))))


(defn- pull-parsed
  [{:as scope :keys [am]}
   pattern e]

  (let [wc? (some (fn [x]
                    (some-> x first (= :wildcard)))
                  pattern)

        links (seq
               (reduce
                (fn [result [tag node]]
                  (if (= tag :map-spec)
                    (merge result node)
                    result)) {} pattern))

        attrs (when-not wc?
                (seq
                 (for [node pattern
                       :let [[tag attr] node]
                       :when (= tag :attr)]
                   attr)))

        attrs* (cond
                 wc? (resolve-attrs scope e)
                 attrs attrs
                 :else (/ 0 0))

        p1 (-pull scope attrs* e)

        ]


    (reduce
     (fn [p [attr pattern]]
       (let [[_ pattern] pattern
             multiple? (am/multiple? am attr)
             es (if multiple?
                  (seq (mapcat attr p))
                  (seq (map attr p)))]
         (if es
           (let [p2 (pull-parsed scope pattern es)]
             (pull-join p p2 attr multiple?))
           p)))
     p1
     links)))


(defn pull
  [scope
   pattern e]
  (let [parsed (s/conform ::ds/pattern pattern)]
    (if (= parsed ::s/invalid)
      (error! "Wrong pull pattern: %s" pattern)
      (pull-parsed scope parsed e))))


#_
(do

  (def _attrs
    [{:db/ident       :artist/name
      :db/valueType   :db.type/string
      :db/cardinality :db.cardinality/one}

     {:db/ident       :release/artist
      :db/valueType   :db.type/ref
      :db/cardinality :db.cardinality/one}

     {:db/ident       :release/year
      :db/valueType   :db.type/integer
      :db/cardinality :db.cardinality/one}

     {:db/ident       :release/tag
      :db/valueType   :db.type/string
      :db/cardinality :db.cardinality/many}])

  (def _db
    {:dbtype "postgresql"
     :dbname "test"
     :host "127.0.0.1"
     :user "ivan"
     :password "ivan"})

  (def _scope
    {:am (am/manager _attrs)
     :en (en/engine _db)}))
