(ns domic.core2
  (:require

   [clojure.string :as str]
   [clojure.spec.alpha :as s]

   [domic.util :refer [sym-generator]]
   [domic.error :refer [error!]]
   [domic.db :as db]
   [domic.var-manager :as vm]
   [domic.query-builder :as qb]
   [domic.attr-manager :as am]
   [domic.db-manager :as dm]
   [domic.util :refer [join zip]]

   [honeysql.core :as sql]
   [datomic-spec.core :as ds]))


(def table
  [[5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]
   [5 6 7 8]])


#_
(def q
  '
  [:find

   ;; [?e ?r "test"]

   ;; ?e .

   ;; [?e ?r]

   ;; [?e ...]

   ?e ?r

   ;; todo add _

   ;; :in $ $foo $bar ?name [?x ?y ?z] [?index ...] [[?a _ ?c]]

   :in $ $foo ?name

   :where

   [?e :artist/name ?name]

   ;; [?e :artist/name ?c]
   [?r :release/artist ?e]
   ;; [?r :release/year ?year]

   ;; [(get-else $ ?artist :artist/startYear "N/A") ?year]

   ;; [?r :release/year ?index]

   #_
   (not
    [?r :release/year ?year]
    [(> ?year 1970)])

   #_
   (or
    [?e :artist/name "Abba"]
    [?e :artist/name "Queen"])

   #_
   [(> ?year ?e)]])


#_
(def q
  '
  [:find (max ?e) ?a ?b ?c
   :in $ $foo ?name
   :where
   [$ ?e :artist/name ?name]
   [$foo ?a ?b ?c]
   ])


(def q
  '
  [:find (max ?e) ?name (min ?t)
   :in $ ?name
   :where
   [$ ?e :artist/name ?name]
   [$ ?e _ _ ?t]])


(def parsed
  (s/conform ::ds/query q))


(defn kw->str [kw]
  (-> kw str (subs 1)))


(defn ->pred-arg
  [arg vm]
  (let [[tag arg] arg]
    (case tag

      :cst
      (let [[_ arg] arg]
        arg)

      :var
      (if (vm/bound? vm arg)
        (vm/get-val vm arg)
        (error! "Var %s is not bound" arg)))))


(defn add-predicate
  [expression vm qb]
  (let [{:keys [expr]} expression
        {:keys [pred args]} expr
        [tag pred] pred]
    (case tag
      :sym
      (case pred
        (= > < >= <= != <>)
        (do
          (when-not (= (count args) 2)
            (error! "Predicate %s should take two args" pred))
          (let [[arg1 arg2] args
                where [pred
                       (->pred-arg arg1 vm)
                       (->pred-arg arg2 vm)]]
            (qb/add-where qb where)))))))


(defprotocol IDBActions

  (add-pattern [db expression scope]))


(extend-protocol IDBActions

  db/DBPG

  (add-pattern [db expression {:keys [qb sg am]}]

    (let [{:keys [elems]} expression
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

      (qb/add-from qb [as layer])

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
              (vm/bind! vm elem fq-field :where elems nil))

            (error! "No matching clause: %s" elem*))))))

  db/DBTable

  (add-pattern [db expression {:keys [qb sg vm]}]

    (let [{:keys [elems]} expression
          layer (sg "t")]

      (qb/add-from qb [as layer])

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
              (vm/bind! vm elem fq-field :where elems nil))

            (error! "No matching clause: %s" elem*))))))








  )


#_
(defn add-pattern
  [expression vm qb am dm]
  (let [{:keys [src-var]} expression
        db (if src-var
             (dm/get-db! dm src-var)
             (dm/default-db! dm))]
    (db/add-pattern db expression vm qb am)))


#_
(defn add-pattern-table
  [db expression vm qb am dm sg]

  #_
  (let [{:keys [elems]} expression
        layer (sg "t")]

    (qb/add-from qb [as layer])

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
            (vm/bind! vm elem fq-field :where elems nil))

          (error! "No matching clause: %s" elem*)))))


  )


#_
(defn add-pattern
  [db expression vm qb am dm]

  (let [{:keys [src-var]} expression
        db (if src-var
             (dm/get-db! dm src-var)
             (dm/default-db! dm))]

    (cond

      ;; (db/db-pg? db)
      ;; (add-pattern-pg expression db vm qb am dm)

      (db/db-table? db)
      (add-pattern-table expression db vm qb am dm)

      :else
      (error! "dunno")))


  #_

  (let [{:keys [src-var]} expression
        db (if src-var
             (dm/get-db! dm src-var)
             (dm/default-db! dm))]
    (db/add-pattern db expression vm qb am)))


(defn add-clause
  [clause vm qb am dm]
  (let [[tag expression] clause]
    (case tag

      ;; :fn-expr

      :pred-expr
      (add-predicate expression vm qb)

      :data-pattern
      (add-pattern expression vm qb am dm))))


(defn add-find-elem
  [find-elem* {:keys [vm qb sg]}]
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


(defn find-elem-agg?
  [find-elem]
  (let [[tag _] find-elem]
    (= tag :agg)))


(defn process-find
  [find-spec vm qb]
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

        are-aggs? (map find-elem-agg? find-elem-list)
        has-agg? (some identity are-aggs?)]

    (doseq [[find-elem agg?] (zip find-elem-list are-aggs?)]
      (let [alias (add-find-elem find-elem vm qb)]
        (when (and has-agg? (not agg?))
          (qb/add-group-by qb alias))))))


(defn process-in
  [inputs {:keys [vm qb dm sg]} params]

  (when-not (= (count inputs) (count params))
    (error! "The number of inputs != the number of parameters"))

  (doseq [[input-src param] (zip inputs params)]
    (let [[tag input] input-src]
      (case tag

        :src-var
        (let [db (db/->db param qb)]
          ;; (db/db-init db qb)
          (dm/add-db dm input db))

        :binding
        (let [[tag input] input]
          (case tag

            :bind-rel
            (let [[input] input
                  fields (for [_ input] (sg "f"))
                  as (sg "coll")
                  alias (sql/raw (format "%s (%s)" as (join fields)))
                  from [{:values param} alias]]

              (qb/add-from qb from)

              (doseq [[input field] (zip input fields)]
                (let [[tag input] input]
                  (case tag
                    :var
                    (vm/bind! vm input field :in input-src nil)
                    :unused nil))))

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
                    (vm/bind! vm input param :in input-src (type param-el))))))

            :bind-scalar
            (vm/bind! vm input param :in input-src nil)))))))


(defn process-where
  [clauses {:as scope :keys [vm qb am dm]}]
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
                    (add-clause clause vm qb am)))))))

        :not-clause
        (qb/with-where-not-and qb
          (let [{:keys [clauses]} clause]
            (doseq [clause clauses]
              (let [[tag clause] clause]
                (case tag
                  :expression-clause
                  (add-clause clause vm qb am))))))

        :expression-clause
        (add-clause clause vm qb am dm)))))


(def PG (db/db-pg))


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

    (process-in inputs scope query-inputs)
    (process-where clauses scope)
    (process-find spec scope)

    (qb/format qb)))
