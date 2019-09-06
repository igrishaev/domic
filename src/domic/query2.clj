(ns domic.query2

  (:require [clojure.spec.alpha :as s]

            [domic.util :refer [sym-generator]]
            [domic.error :refer [error!]]
            [domic.db :as db]
            [domic.var-manager :as vm]
            [domic.query-builder :as qb]
            [domic.db-manager :as dm]
            [domic.util :refer [join zip]]
            [domic.db :as db]
            [domic.query-params :as qp]
            [domic.engine :as en]

            [honeysql.core :as sql]
            [datomic-spec.core :as ds])

  (:import [domic.db DBPG DBTable]))



(def table
  [[5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]])


(def q
  '
  [:find ?e ?name ?y
   :in $ ?name
   :where
   [$ ?e :artist/name ?name]
   [$ ?r :release/artist ?e]
   [$ ?r :release/year ?y]
   (not
    [$ ?r :release/year 1985])])


(def q
  '
  [:find ?e ?name ?y
   :in $ ?name
   :where
   [$ ?e :artist/name ?name]
   [$ ?r :release/artist ?e]
   [$ ?r :release/year ?y]
   (or
    [$ ?r :release/year 1985]
    [$ ?r :release/year 1986])])


(def q
  '
  [:find ?e ?name ?y
   :in $ ?name
   :where
   [$ ?e :artist/name _]
   [$ ?r :release/artist ?e]
   [$ ?r :release/year ?y]
   (or
    [$ ?r :release/year 1985]
    [$ ?r :release/year 1986]

    )])


#_
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
          {:keys [qb sg vm qp]} scope
          {:keys [elems]} expression
          layer (sg "d")]

      (qb/add-from qb [alias layer])

      (doseq [[elem* field] (zip elems fields)]

        (let [[tag elem] elem*

              fq-field (sql/qualify layer field)]

          (case tag

            :blank nil

            :cst
            (let [[tag value] elem]
              (let [_a (sg (str field))
                    _p (sql/param _a)
                    where [:= fq-field _p]]
                (qp/add-param qp _a value)
                (qb/add-where qb where)))

            :var
            (if (vm/bound? vm elem)
              (let [_v (vm/get-val vm elem)
                    where [:= fq-field _v]]
                (qb/add-where qb where))
              (vm/bind! vm elem fq-field))))))


    #_
    (let [{:keys [alias fields]} db
          {:keys [qb sg am vm]} scope

          {:keys [elems]} expression

          layer (sg "d")]

      (qb/add-from qb [alias layer])

      (doseq [[elem* field] (zip elems fields)]

        (let [[tag elem] elem*

              fq-field
              (cond-> (sql/qualify layer field)
                (and (= field 'v) pg-type (not= pg-type :text))
                (cast pg-type))]

          (case tag

            :blank nil

            :cst
            (let [[tag v] elem]
              (let [ ;; param (sg "?")
                    where [:= fq-field v

                           #_
                           param

                           #_
                           (sql/param param)
                           ]]
                ;; (qb/add-param qb param v)
                (qb/add-where qb where)))

            :var
            (if (vm/bound? vm elem)
              (let [where [:= fq-field (vm/get-val vm elem)]]
                (qb/add-where qb where))
              (vm/bind! vm elem fq-field)))))))

  DBTable

  (init-db [db {:keys [qb]}]
    (let [{:keys [data alias fields]} db
          with [[alias {:columns fields}] {:values data}]]
      (qb/add-with qb with)))

  (add-pattern-db [db scope expression]

    #_
    (let [{:keys [alias fields]} db
          {:keys [qb sg vm]} scope
          {:keys [elems]} expression
          layer (sg "t")]

      (qb/add-from qb [alias layer])

      (doseq [[elem* field] (zip elems fields)]

        (let [[tag elem] elem*

              fq-field
              (sql/qualify layer field)]

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

            :bind-rel nil
            #_
            (let [arity (-> input first count)
                  fields (for [_ (first input)] (sg "f"))
                  alias [(sg "coll") (sql/raw (format "(%s)" (join fields)))]
                  from [{:values param} alias]]

              (qb/add-from qb from)

              (doseq [[input field] (zip input fields)]
                (let [[tag input] input]
                  (case tag
                    :unused nil
                    :var
                    (vm/bind! vm input field :in input-src nil)))))

            :bind-coll nil
            #_
            (let [{:keys [var]} input
                  as (sg "coll")
                  field (sg "f")
                  values {:values (mapv vector param)}
                  alias (sql/raw (format "%s (%s)" as field))
                  from [values alias]]
              (vm/bind! vm var field :in input-src nil)
              (qb/add-from qb from))

            :bind-tuple nil
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
            (let [_a (sg (name input))
                  _p (sql/param _a)]
              (qp/add-param qp _a param)
              (vm/bind! vm input _p))))))))


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


(defn process-not-clause
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


(defn process-or-clause
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

                  :not-clause
                  (process-not-clause scope clause)

                  :expression-clause
                  (add-clause scope clause))))))))))


(defn process-where
  [{:as scope :keys [qb]} clauses]
  (doseq [clause clauses]
    (let [[tag clause] clause]
      (case tag

        :or-clause
        (process-or-clause scope clause)

        :not-clause
        (process-not-clause scope clause)

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
        group? (some identity aggs?)

        ]

    (doseq [[find-elem agg?] (zip find-elem-list aggs?)]
      (let [alias (add-find-elem scope find-elem)]
        (when (and group? (not agg?))
          (qb/add-group-by qb alias))))))


(defn aaa
  [query-parsed & query-inputs]

  (let [db-spec {:dbtype "postgresql"
                 :dbname "test"
                 :host "127.0.0.1"
                 :user "ivan"
                 :password "ivan"}

        en (en/engine db-spec)

        sg (sym-generator)
        vm (vm/manager)
        qb (qb/builder)
        qp (qp/params)
        dm (dm/manager)

        scope {:sg sg :vm vm :qb qb :dm dm :qp qp}

        {:keys [find in where]} query-parsed
        {:keys [inputs]} in
        {:keys [spec]} find
        {:keys [clauses]} where]

    (process-in scope inputs query-inputs)
    (process-where scope clauses)
    (process-find scope spec)

    (clojure.pprint/pprint (qb/->map qb))

    #_
    (clojure.pprint/pprint (qb/format qb (qp/get-params qp)))

    (qb/set-distinct qb)

    (let [params (qp/get-params qp)
          [query & args] (qb/format qb params)
          pg-args (mapv en/->pg args)]
      (en/query en (into [query] pg-args)))

    ;; (en/query en (qb/format qb))

    ))
