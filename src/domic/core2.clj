(ns domic.core2
  (:require [datomic-spec.core :as ds]

            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [honeysql.core :as sql]

            [domic.var-manager :as vm]
            [domic.query-builder :as qb]
            [domic.attr-manager :as am]

            ))


(def q
  '
  [:find

   ;; [?e ?r "test"]

   ;; ?e .

   ;; [?e ?r]

   ;; [?e ...]

   ?e ?r

   ;; todo add _

   :in $ ?name [?x ?y ?z] [?index ...] [[?a _ ?c]]
   :where

   [?e :artist/name ?name]

   [?e :artist/name ?c]
   ;; [?r :release/artist ?e]
   ;; [?r :release/year ?year]

   [?r :release/year ?index]

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


(def parsed
  (s/conform ::ds/query q))


(defn error!
  [message & args]
  (throw (new Exception ^String (apply format message args))))


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


(defn add-pattern
  [expression vm qb am]
  (let [{:keys [src-var
                elems]} expression
        [e a v t] elems

        prefix (str (gensym "d"))

        attr
        (let [[tag a] a]
          (case tag
            :cst
            (let [[tag a] a]
              (case tag
                :kw a))))

        pg-type (am/get-pg-type am attr)]

    (qb/add-from qb [:datoms (keyword prefix)])

    ;; E
    (let [[tag e] e]
      (case tag
        :var
        (let [sql (keyword (format "%s.e" prefix))]
          (if (vm/bound? vm e)
            (let [where [:= sql (vm/get-val vm e)]]
              (qb/add-where qb where))
            (vm/bind vm e :where sql)))))

    ;; A
    (let [where [:= (sql/raw (format "%s.a" prefix)) (kw->str attr)]]
      (qb/add-where qb where))

    ;; V
    (let [[tag v] v]

      (let [sql (sql/raw (format "%s.v::%s" prefix pg-type))]

        (case tag
          :cst
          (let [[tag v] v]
            (let [where [:= sql v]]
              (qb/add-where qb where)))

          :var
          (if (vm/bound? vm v)
            (let [where [:= sql (vm/get-val vm v)]]
              (qb/add-where qb where))
            (vm/bind vm v :where sql)))))))


(defn add-clause
  [clause vm qb am]
  (let [[tag expression] clause]
    (case tag
      :pred-expr
      (add-predicate expression vm qb)
      :data-pattern
      (add-pattern expression vm qb am))))


(defn find-add-elem
  [elem vm qb]
  (let [[tag elem] elem]
    (case tag
      :var
      (if (vm/bound? vm elem)
        (let [val (vm/get-val vm elem)]
          (qb/add-select qb val))
        (error! "Var %s is not bound" elem)))))


(defn process-find
  [spec vm qb]
  (let [[tag spec] spec]
    (case tag

      :coll
      (let [{:keys [elem]} spec]
        (find-add-elem elem vm qb))

      ;; https://github.com/alexanderkiel/datomic-spec/issues/6
      ;; :tuple

      :scalar
      (let [{:keys [elem]} spec]
        (find-add-elem elem vm qb))

      :rel
      (doseq [elem spec]
        (find-add-elem elem vm qb)))))


(def zip (partial map vector))


(defn process-in
  [inputs vm qb params]

  (when-not (= (count inputs) (count params))
    (error! "The number of inputs != the number of parameters"))

  (doseq [[input param] (zip inputs params)]
    (let [[tag input] input]
      (case tag

        :src-var
        (println input)

        :binding
        (let [[tag input] input]
          (case tag

            :bind-rel
            (let [[input] input
                  var-alias-list (for [_ input]
                                   (gensym "var"))

                  values {:values param}
                  values-name (gensym "coll")
                  values-alias (sql/raw (format "%s (%s)" values-name (str/join ", " var-alias-list)))
                  from [values values-alias]]

              (qb/add-from qb from)

              (doseq [[input var-alias] (zip input var-alias-list)]
                (let [[tag input] input]
                  (case tag
                    :var
                    (let []
                      (vm/bind vm input :in var-alias))
                    :unused nil))))


            :bind-coll
            (let [{:keys [var]} input
                  alias (gensym "coll")
                  alias-var (gensym "coll_var")
                  from-values {:values (mapv vector param)}
                  from-alias (sql/raw
                              (format "as %s (%s)" alias alias-var))
                  from [from-values from-alias]]

              ;; check if bound
              (vm/bind vm var :in alias-var)
              (qb/add-from qb from))


            :bind-tuple
            (do
              (when-not (= (count input)
                           (count param))
                (error! "Arity mismatch: %s != %s" input param))

              (doseq [input input]
                (let [[tag input] input]
                  (case tag
                    :var
                    ;; check if already bound
                    (vm/bind vm input :in param)))))

            :bind-scalar
            (let []
              (vm/bind vm input :in param))))))))


(defn process-where
  [clauses vm qb am]
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
        (add-clause clause vm qb am)))))


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

        am (am/manager attrs)
        vm (vm/manager)
        qb (qb/builder)

        {:keys [find in where]} query-parsed

        {:keys [inputs]} in
        {:keys [spec]} find
        {:keys [clauses]} where]

    (process-in inputs vm qb query-inputs)
    (process-where clauses vm qb am)
    (process-find spec vm qb)

    (qb/->map qb)))
