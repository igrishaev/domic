(ns domic.query2

  (:require [clojure.spec.alpha :as s]

            [domic.sql-helpers :refer
             [->cast as-fields as-field lookup?]]
            [domic.runtime :refer [resolve-lookup!]]
            [domic.util :refer [sym-generator]]
            [domic.error :refer
             [error! error-case!]]
            [domic.db :as db]
            [domic.var-manager :as vm]
            [domic.query-builder :as qb]
            [domic.db-manager :as dm]
            [domic.util :refer [zip]]
            [domic.db :as db]
            [domic.query-params :as qp]
            [domic.attr-manager :as am]
            [domic.engine :as en]

            [honeysql.core :as sql]
            [datomic-spec.core :as ds])

  (:import [domic.db DBPG DBTable]))

#_
(def query
  '
  [:find ?name ?a ?x ?y ?m ?p
   :in $ $data ?name [?x ...] ?y [[?m _ ?p]]
   :where
   [$ ?a :artist/name ?name]])


#_
(def query
  '
  [:find ?x
   :in $ $data
   :where
   [$data  _ ?b]
   [$data ?b ?x]])


(def query
  '
  [:find ?r ?y ?name ;; ?y1 ?y2 ?y3 ?y4
   :in $ ?a
   :where
   [$ ?r :release/artist ?a]
   [$ ?r :release/year ?y]
   [$ ?a :artist/name ?name]
   [(= ?y 1991)]

   ;; [(in ?y 1985 1986 1987)]

   ;; [(+ ?y 100) ?y1]
   ;; [(- ?y 100) ?y2]
   ;; [(* ?y 100) ?y3]
   ;; [(/ ?y 100) ?y4]
   #_
   [(foo_bar ?y 1 2 3) ?y5]

   #_
   [$ ?a :db/ident :metallica]])


(defprotocol IDBActions

  (init-db [db scope])

  (add-pattern-db [db scope expression]))


(defn- find-attr
  [field-elem-pairs]
  (when-let [elem (->> field-elem-pairs
                       (into {})
                       :a)]
    (let [[tag elem] elem]
      (when (= tag :cst)
        (let [[tag elem] elem]
          (when (= tag :kw)
            elem))))))


(extend-protocol IDBActions

  DBPG

  (init-db [db scope])

  (add-pattern-db [db scope expression]

    (let [{:keys [qb sg vm qp am]} scope
          {:keys [alias fields]} db
          {:keys [elems]} expression
          alias-layer (sg "layer")

          field-elem-pairs (zip fields elems)

          attr (find-attr field-elem-pairs)
          pg-type (when attr
                    (am/get-pg-type am attr))]

      (qb/add-from qb [alias alias-layer])

      (doseq [[field elem*] field-elem-pairs]

        (let [[tag elem] elem*
              v? (= field :v)

              alias-fq (sql/qualify alias-layer field)
              alias-fq (if (and v? attr)
                         (->cast alias-fq pg-type)
                         alias-fq)]

          (case tag

            :cst
            (let [[tag v] elem]
              (let [alias-v (sg "v")
                    param (sql/param alias-v)
                    where [:= alias-fq param]]
                (qp/add-param qp alias-v v)
                (qb/add-where qb where)))

            :var
            (if (vm/bound? vm elem)
              (let [where [:= alias-fq (vm/get-val vm elem)]]
                (qb/add-where qb where))
              (vm/bind! vm elem alias-fq))

            :blank nil

            (error-case! elem*))))))

  DBTable

  (init-db [db {:keys [qb]}]
    (let [{:keys [alias fields data]} db
          with [[alias {:columns fields}] {:values data}]]
      (qb/add-with qb with)))

  (add-pattern-db [db scope expression]

    (let [{:keys [qb sg vm]} scope
          {:keys [alias fields]} db
          {:keys [elems]} expression
          alias-layer (sg "layer")]

      (qb/add-from qb [alias alias-layer])

      (doseq [[elem* field] (zip elems fields)]
        (let [[tag elem] elem*
              alias-fq (sql/qualify alias-layer field)]

          (case tag

            :cst
            (let [[tag v] elem]
              (let [where [:= alias-fq v]]
                (qb/add-where qb where)))

            :var
            (if (vm/bound? vm elem)
              (let [where [:= alias-fq (vm/get-val vm elem)]]
                (qb/add-where qb where))
              (vm/bind! vm elem alias-fq))

            :blank nil

            (error-case! elem*)))))))


(defn- discover-db
  [{:keys [sg]} var data]
  (cond
    (db/pg? data) data
    (coll? data)
    (let [[row] data
          alias-coll (sg "data")
          alias-fields (for [_ row] (sg "f"))]
      (db/table alias-coll alias-fields data))
    :else
    (error! "Wrong database: %s" var)))


(defn- process-in
  [{:as scope :keys [vm qb dm sg qp]}
   inputs params]

  (let [n-inputs (count inputs)
        n-params (count params)]
    (when-not (= n-inputs n-params)
      (error! "Arity mismatch: %s input(s) and %s param(s)"
              n-inputs n-params)))

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
            (let [[input] input
                  alias-coll (sg "data")
                  alias-fields (for [_ input] (sg "f"))
                  alias-full (as-fields alias-coll alias-fields)
                  from [{:values param} alias-full]]

              (qb/add-from qb from)

              (doseq [[input alias-field]
                      (zip input alias-fields)]
                (let [[tag input] input
                      alias-fq (sql/qualify alias-coll alias-field)]
                  (case tag
                    :unused nil
                    :var
                    (vm/bind! vm input alias-fq)))))

            :bind-coll
            (let [{:keys [var]} input
                  alias-coll (sg "coll")
                  alias-field (sg "field")
                  alias-full (as-field alias-coll alias-field)
                  field (sql/qualify alias-coll alias-field)
                  values {:values (mapv vector param)}
                  from [values alias-full]]
              (vm/bind! vm var field)
              (qb/add-from qb from))

            :bind-tuple nil ;; bug in datomic spec
            #_
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
            (let [value (if (lookup? param)
                          (resolve-lookup! scope param)
                          param)]
              (let [_a (sg (name input))
                    _p (sql/param _a)]
                (qp/add-param qp _a value)
                (vm/bind! vm input _p)))))))))


(defn- add-pattern
  [{:as scope :keys [dm]} expression]
  (let [{:keys [src-var]} expression
        db (if src-var
             (dm/get-db! dm src-var)
             (dm/default-db! dm))]
    (add-pattern-db db scope expression)))


(defn- add-predicate
  [scope expression]

  (let [{:keys [qb vm sg qp]} scope
        {:keys [expr]} expression
        {:keys [pred args]} expr

        [pred-tag pred] pred

        args* (vec
               (for [arg args]
                 (let [[tag arg] arg]
                   (case tag
                     :var
                     (vm/get-val! vm arg)

                     :cst
                     (let [[tag arg] arg]
                       (let [param (sg "param")]
                         (qp/add-param qp param arg)
                         (sql/param param)))))))

        pred-expr
        (case pred
          ;; :in [:in (first args*) (rest args*)]
          (into [pred] args*))]

    (case pred-tag
      :sym
      (qb/add-where qb pred-expr))))


(defn- add-function
  [{:as scope :keys [qb vm sg qp]}
   expression]

  (let [{:keys [expr binding]} expression
        {:keys [fn args]} expr

        args* (for [arg args]
                (let [[tag arg] arg]
                  (case tag
                    :var
                    (vm/get-val! vm arg)

                    :cst
                    (let [[tag arg] arg]
                      (let [param (sg "param")]
                        (qp/add-param qp param arg)
                        (sql/param param))))))

        [fn-tag fn] fn
        [bind-tag binding] binding

        fn-expr
        (case fn-tag
          :sym
          (apply sql/call fn args*))]

    (case bind-tag
      :bind-scalar
      (vm/bind! vm binding fn-expr))))


(defn- add-clause
  [scope clause]
  (let [[tag expression] clause]
    (case tag
      :fn-expr
      (add-function scope expression)
      :pred-expr
      (add-predicate scope expression)
      :data-pattern
      (add-pattern scope expression))))


#_
(defn- process-not-clause
  [{:as scope :keys [qb]} clause]

  (let [qb* (qb/builder)
        scope* (assoc scope :qb qb*)]

    (let [{:keys [clauses]} clause]
      (doseq [clause clauses]
        (let [[tag clause] clause]
          (case tag
            :expression-clause
            (add-clause scope* clause)))))

    (qb/add-select qb* (sql/inline 1))
    (qb/add-where qb [:not {:exists (qb/->map qb*)}])))

#_
(defn- process-or-clause
  [{:as scope :keys [qb]} clause]
  (qb/with-where-or qb
    (let [{:keys [clauses]} clause]
      (doseq [clause clauses]
        (qb/with-where-and qb
          (let [[tag clause] clause]
            (case tag
              :clause
              (let [[tag clause] clause]
                (case tag

                  ;; :not-clause
                  ;; (process-not-clause scope clause)

                  :expression-clause
                  (add-clause scope clause))))))))))


(defn- process-where
  [{:as scope :keys [qb]} clauses]
  (doseq [clause clauses]
    (let [[tag clause] clause]
      (case tag
        ;; :or-clause
        ;; (process-or-clause scope clause)
        ;; :not-clause
        ;; (process-not-clause scope clause)
        :expression-clause
        (add-clause scope clause)))))


(defn find-elem-agg?
  [find-elem]
  (let [[tag _] find-elem]
    (= tag :agg)))


(defn- add-find-elem
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
                              :var
                              (vm/get-val! vm arg)))))]
        (qb/add-select qb [call alias]))

      :var
      (let [val (vm/get-val! vm find-elem)]
        (qb/add-select qb [val alias]))

      (error! "No matching clause: %s" find-elem*))

    alias))


(defn- get-find-type
  [find-parsed]
  (let [[tag _] find-parsed]
    tag))


(defn- process-find
  [{:as scope :keys [vm qb]}
   find-spec]

  (let [[tag find-spec] find-spec

        find-elem-list
        (case tag

          ;; :tuple
          ;; https://github.com/alexanderkiel/datomic-spec/issues/6

          (:coll :scalar)
          (let [{:keys [elem]} find-spec]
            [elem])

          :rel find-spec)

        aggs? (map find-elem-agg? find-elem-list)
        group? (some identity aggs?)

        ]

    (doseq [[find-elem agg?] (zip find-elem-list aggs?)]
      (let [alias (add-find-elem scope find-elem)]
        (when (and group? (not agg?))
          (qb/add-group-by qb alias))))))


(defn- as-vector
  [{:as scope :keys [en]}
   query]
  (rest (en/query en query {:as-arrays? true})))


(defn- q-internal
  [{:as scope :keys [en am]}
   query-parsed
   & query-inputs]

  (let [scope (assoc scope
                     :sg (sym-generator)
                     :vm (vm/manager)
                     :qb (qb/builder)
                     :dm (dm/manager)
                     :qp (qp/params))

        {:keys [qb qp]} scope

        {:keys [find in where]} query-parsed
        {:keys [inputs]} in
        {:keys [spec]} find
        {:keys [clauses]} where

        find-type (get-find-type spec)]

    (case find-type
      :scalar
      (qb/set-limit qb (sql/inline 1))
      nil)

    (process-in scope inputs query-inputs)
    (process-where scope clauses)
    (process-find scope spec)

    (qb/set-distinct qb)

    (qb/debug qb (qp/get-params qp))

    (let [params (qp/get-params qp)
          query (qb/format qb params)
          result (as-vector scope query)]

      (case find-type
        :coll (map first result)
        :scalar (ffirst result)
        result))))


(defn- parse-query
  [query-list]
  (let [result (s/conform ::ds/query query-list)]
    (if (= result ::s/invalid)
      (error! "Cannot parse query: %s" query-list)
      result)))


(defn q [scope query-list & args]
  (apply q-internal
         scope
         (parse-query query-list)
         args))

#_
(q _scope query (db/pg) [:db/ident :metallica])

#_
(do

  (def _attrs
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
