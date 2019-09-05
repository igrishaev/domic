(ns domic.query

  (:require [clojure.spec.alpha :as s]

            [domic.util :refer [sym-generator]]
            [domic.error :refer [error!]]
            [domic.db :as db]
            [domic.var-manager :as vm]
            [domic.query-builder :as qb]
            [domic.attr-manager :as am]
            [domic.db-manager :as dm]
            [domic.util :refer [join zip]]
            [domic.db :as db]

            [honeysql.core :as sql]
            [datomic-spec.core :as ds]

            )

  (:import [domic.db DBPG DBTable])

  )


(def table
  [[5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]])


(def q
  '
  [:find ?e
   :in $ ?name
   :where
   [$ ?e :artist/name ?name]])


(def q
  '
  [:find (max ?e) ?a ?b ?c
   :in $ $foo ?name
   :where
   [$ ?e :artist/name ?name]
   [$foo ?a ?b ?c]])


(def parsed
  (s/conform ::ds/query q))


(defprotocol IDBActions

  (init-db [db scope])

  (add-pattern-db [db scope expression]))


(extend-protocol IDBActions

  DBPG

  (init-db [db scope])

  (add-pattern-db [db scope expression]

    (let [{:keys [alias fields]} db
          {:keys [qb sg am vm]} scope

          {:keys [elems]} expression
          [_ A] elems
          layer (sg "d")

          attr
          (when A
            (let [[tag a] A]
              (case tag
                :cst
                (let [[tag a] a]
                  (case tag
                    :kw a))
                nil)))

          type-pg
          (when attr
            (am/get-pg-type am attr))]

      (qb/add-from qb [alias layer])

      (doseq [[elem* field] (zip elems fields)]

        (let [[tag elem] elem*

              cast
              (when (and (= field 'v)
                         (some? type-pg)
                         (not= type-pg "text"))
                (format "::%s" type-pg))

              fq-field
              (sql/raw
               (format "%s.%s%s" layer field (or cast "")))]

          (case tag

            :blank nil

            :cst
            (let [[tag v] elem]
              (let [where [:= fq-field v]]
                (qb/add-where qb where)))

            :var
            (if (vm/bound? vm elem)
              (let [where [:= fq-field (vm/get-val vm elem)]]
                (qb/add-where qb where))
              (vm/bind! vm elem fq-field :where elems nil)))))))

  DBTable

  (init-db [db {:keys [qb]}]
    (let [{:keys [data alias fields]} db
          with [[alias {:columns fields}] {:values data}]]
      (qb/add-with qb with)))

  (add-pattern-db [db scope expression]

    (let [{:keys [alias fields]} db
          {:keys [qb sg vm]} scope
          {:keys [elems]} expression
          layer (sg "t")]

      (qb/add-from qb [alias layer])

      (doseq [[elem* field] (zip elems fields)]

        (let [[tag elem] elem*
              fq-field (sql/raw (format "%s.%s" layer field))]

          (case tag

            :cst
            (let [[tag v] elem]
              (let [where [:= fq-field v]]
                (qb/add-where qb where)))

            :var
            (if (vm/bound? vm elem)
              (let [where [:= fq-field (vm/get-val vm elem)]]
                (qb/add-where qb where))
              (vm/bind! vm elem fq-field :where elems nil))))))))


(defn discover-db
  [{:keys [sg]} var db]
  (cond
    (db/db-pg? db) db

    (coll? db)
    (let [alias (sg "t")
          arity (-> db first count)
          fields (for [_ (first db)]
                   (sg "f"))]
      (db/db-table db alias fields))

    :else
    (error! "Wrong database: %s" var)))


(defn process-in
  [{:as scope :keys [vm qb dm sg]}
   inputs params]

  (when-not (= (count inputs) (count params))
    (error! "Arity mismatch: %s input(s) and %s param(s)"
            (count inputs) (count params)))

  (doseq [[input-src param] (zip inputs params)]
    (let [[tag input] input-src]
      (case tag

        :src-var
        (let [db (discover-db scope input param)]
          (init-db db scope)
          (dm/add-db dm input db))

        :binding
        (let [[tag input] input]
          (case tag

            :bind-rel
            (let [arity (-> input first count)
                  fields (for [_ (first input)]
                           (sg "f"))
                  as (sg "coll")
                  alias (sql/raw
                         (format "%s (%s)" as (join fields)))
                  from [{:values param} alias]]

              (qb/add-from qb from)

              (doseq [[input field] (zip input fields)]
                (let [[tag input] input]
                  (case tag
                    :unused nil
                    :var
                    (vm/bind! vm input field :in input-src nil)))))

            :bind-coll
            (let [{:keys [var]} input
                  as (sg "coll")
                  field (sg "f")
                  values {:values (mapv vector param)}
                  alias (sql/raw (format "%s (%s)" as field))
                  from [values alias]]
              (vm/bind! vm var field :in input-src nil)
              (qb/add-from qb from))

            :bind-tuple
            (do
              (when-not (= (count input)
                           (count param))
                (error! "Arity mismatch: %s != %s" input param))

              (doseq [[input param-el] (zip input param)]
                (let [[tag input] input]
                  (case tag
                    :var
                    (vm/bind! vm input param
                              :in input-src (type param-el))))))

            :bind-scalar
            (vm/bind! vm input param :in input-src nil)))))))


(defn add-pattern
  [{:as scope :keys [dm]} expression]
  (let [{:keys [src-var]} expression
        db (if src-var
             (dm/get-db! dm src-var)
             (dm/default-db! dm))]
    (add-pattern-db db scope expression)))


(defn add-clause
  [scope clause]

  (let [[tag expression] clause]
    (case tag

      ;; :fn-expr

      ;; :pred-expr
      ;; (add-predicate scope expression)

      :data-pattern
      (add-pattern scope expression))))


(defn process-where
  [{:as scope :keys [qb]} clauses]
  (doseq [clause clauses]
    (let [[tag clause] clause]
      (case tag

        :or-clause
        (qb/with-where-or qb
          (let [{:keys [clauses]} clause]
            (doseq [clause clauses]
              (let [[tag clause] clause]
                (case tag
                  :clause
                  (let [[tag clause] clause]
                    :expression-clause
                    (add-clause scope clause)))))))

        :not-clause
        (qb/with-where-not-and qb
          (let [{:keys [clauses]} clause]
            (doseq [clause clauses]
              (let [[tag clause] clause]
                (case tag
                  :expression-clause
                  (add-clause scope clause))))))

        :expression-clause
        (add-clause scope clause)))))


(defn find-elem-agg?
  [find-elem]
  (let [[tag _] find-elem]
    (= tag :agg)))


(defn add-find-elem
  [{:keys [vm qb sg]} find-elem*]
  (let [[tag find-elem] find-elem*
        alias (sg "f")]

    (case tag

      :agg
      (let [{:keys [name args]} find-elem
            call (apply sql/call name
                        (for [arg args]
                          (let [[tag arg] arg]
                            (case tag
                              :var ;; check if bound
                              (vm/get-val vm arg)))))]
        (qb/add-select qb [call alias]))

      :var
      (let [val (vm/get-val! vm find-elem)]
        (qb/add-select qb [val alias]))

      (error! "No matching clause: %s" find-elem*))

    alias))


(defn process-find
  [{:as scope :keys [vm qb]} find-spec]
  (let [[tag find-spec] find-spec

        find-elem-list
        (case tag

          ;; query meta info

          ;; https://github.com/alexanderkiel/datomic-spec/issues/6
          ;; :tuple

          (:coll :scalar)
          (let [{:keys [elem]} find-spec]
            [elem])

          :rel find-spec)

        aggs? (map find-elem-agg? find-elem-list)
        group? (some identity aggs?)]

    (doseq [[find-elem agg?] (zip find-elem-list aggs?)]
      (let [alias (add-find-elem scope find-elem)]
        (when (and group? (not agg?))
          (qb/add-group-by qb alias))))))


(defn aaa
  [query-parsed & query-inputs]

  (let [attrs [{:db/ident       :artist/name
                :db/valueType   :db.type/string
                :db/cardinality :db.cardinality/one}

               {:db/ident       :release/artist
                :db/valueType   :db.type/ref
                :db/cardinality :db.cardinality/one}

               {:db/ident       :release/year
                :db/valueType   :db.type/integer
                :db/cardinality :db.cardinality/one}]

        sg (sym-generator)
        am (am/manager attrs)
        vm (vm/manager)
        qb (qb/builder)
        dm (dm/manager)

        scope {:sg sg :am am :vm vm :qb qb :dm dm}

        {:keys [find in where]} query-parsed
        {:keys [inputs]} in
        {:keys [spec]} find
        {:keys [clauses]} where]

    (process-in scope inputs query-inputs)
    (process-where scope clauses)
    (process-find scope spec)

    (qb/format qb)))
