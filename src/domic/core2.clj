(ns domic.core2
  (:require [datomic-spec.core :as ds]

            [clojure.spec.alpha :as s]
            [honeysql.core :as sql]

            [domic.var-manager :as vm]
            [domic.query-builder :as qb]
            [domic.attr-manager :as am]

            ))


(def q
  '
  [:find ?e ?r
   :in $ ?name
   :where
   [?e :artist/name ?name]
   [?r :release/artist ?e]
   [?r :release/year ?year]

   (not
    [?r :release/year ?year]
    [(> ?year 1970)])

   (or
    [?e :artist/name "Abba"]
    [?e :artist/name "Queen"])



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
  (let [{:keys [elems]} expression
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


(defn aaa
  [query-parsed]

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

        {:keys [spec]} find
        {:keys [clauses]} where]

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
          (add-clause clause vm qb am))))

    (let [[tag] spec]
      (case tag
        :rel
        (let [[_ find-rel] spec]
          (doseq [find-elem find-rel]
            (let [[tag] find-elem]
              (case tag
                :var
                (let [[_ var] find-elem]
                  (if (vm/bound? vm var)
                    (let [val (vm/get-val vm var)]
                      (qb/add-select qb val))
                    (error! "Var %s is not bound" var)))))))))

    (qb/->map qb)))
