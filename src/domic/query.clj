(ns domic.query

  (:require
   [clojure.spec.alpha :as s]

   [domic.pull :as pull]
   [domic.sql-helpers :as h]
   [domic.runtime :refer [resolve-lookup!]]
   [domic.error :as e]
   [domic.db :as db]
   [domic.var-manager :as vm]
   [domic.query-builder :as qb]
   [domic.pp-manager :as pp]
   [domic.db-manager :as dm]
   [domic.util :as u]
   [domic.db :as db]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]
   [domic.spec-datomic :as ds]

   [honeysql.core :as sql])

  (:import
   [domic.db DBPG DBTable]))


;; todo
;; unify rules
;; unify databases (rename to table/dataset)
;; process with
;; process maps
;; cast datasets to params
;; fix tuple binding
;; deal with pull pattern
;; detect idents


(def rules
  '
  [

   [(artist-name? [?a] ?name)
    [?a :artist/name ?name]]

   #_
   [(short-track [?a ?t] ?len ?max)
    [?t :track/artists ?a]
    [?t :track/duration ?len]
    [(< ?len ?max)]]

   #_
   [(foobar ?len ?max)
    [?t :track/artists ?a]]])


(def query
  '

  [:find ?a ?c
   :in $xs $ys
   :where (or-join [?a ?c]
                   [$xs ?a ?b ?c]
                   [$ys ?a ?c])]


  #_

  [:find ?a ?b ?c
   :in $xs $ys
   :where [$xs ?a ?b ?c]
   (or-join [?a]
            [$ys ?a ?b ?d])]

  #_

  [:find ?e ?name
   :in
   :where



   (or-join [[?e] ?name]
            [?e :artist/name ?name])

   ;; [?e :db/ident :metallica]
   ;; (artist-name? ?e ?n)

   ])


(defn- group-rules
  [rules]
  (let [rules* (s/conform ::ds/rules rules)]
    (when (= rules* ::s/invalid)
      (e/error! "Wrong rules: %s" rules*))
    (into {} (for [rule* rules*]
               [(-> rule* :head :name) rule*]))))


(defn- join-*
  [op clauses]
  (when-let [clauses* (not-empty (filter some? clauses))]
    (into [op] clauses*)))

(def join-and (partial join-* :and))
(def join-or  (partial join-* :or))


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


(defn- finish-layer
  [{:as scope :keys [qb lvl]}
   table where]

  (let [where (join-and where)
        [_ alias-layer] table]

    (if (qb/empty-from? qb)

      (do
        (qb/add-from qb table)
        where)

      (if (zero? lvl)

        (do
          (qb/add-join qb table where)
          nil)

        (do
          (qb/add-left-join qb table where)
          [:is-not alias-layer nil])))))


(extend-protocol IDBActions

  DBPG

  (init-db [db scope])

  (add-pattern-db [db scope expression]

    (let [{:keys [qb sg vm qp am lvl table]} scope
          {:keys [alias fields]} db
          {:keys [elems]} expression

          alias-sub (sg "sub")
          alias-table :d

          field-elem-pairs (u/zip fields elems)

          attr (find-attr field-elem-pairs)
          pg-type (when attr
                    (am/db-type am attr))

          qb-sub (qb/builder)]

      (qb/set-distinct qb-sub)

      (doseq [f fields]
        (qb/add-select qb-sub (sql/qualify alias-table f)))
      (qb/add-from qb-sub [table alias-table])

      (doseq [[field elem*] field-elem-pairs]

        (let [[tag elem] elem*
              v? (= field :v)

              alias-sub-field (sql/inline (sql/qualify alias-sub field))
              alias-sub-field (if (and v? attr)
                                (h/->cast alias-sub-field pg-type)
                                alias-sub-field)

              alias-fq (sql/qualify alias-table field)
              alias-fq (if (and v? attr)
                         (h/->cast alias-fq pg-type)
                         alias-fq)]

          (case tag

            :cst
            (let [[tag v] elem]
              (let [alias-v (sg "v")
                    param (sql/param alias-v)
                    where [:= alias-fq param]]
                (qp/add-param qp alias-v v)
                (qb/add-where qb-sub where)))

            :var
            (if (vm/bound? vm elem)

              (let [val (vm/get-val vm elem)]
                (qb/add-where qb-sub [:= alias-fq val])

                (when-let [src (-> val meta :src)]
                  (qb/add-from? qb-sub src)
                  (qb/add-where qb [:= alias-sub-field val])
                  (qb/add-from? qb alias-sub)))

              (vm/bind vm elem
                       (-> alias-sub-field
                           (with-meta {:src alias-sub}))))

            :blank nil

            ;; else
            (e/error-case! elem*))))

      (qb/add-with qb [alias-sub (qb/->map qb-sub)])
      (qb/add-from? qb alias-sub)))

  DBTable

  (init-db [db {:keys [qb]}]
    (let [{:keys [alias fields data]} db
          with [[alias {:columns fields}] {:values data}]]
      (qb/add-with qb with)))

  (add-pattern-db [db scope expression]

    (let [{:keys [qb sg vm lvl]} scope
          {:keys [alias fields]} db
          {:keys [elems]} expression
          alias-layer (sg "layer")

          where* (transient [])]

      (doseq [[elem* field] (u/zip elems fields)]
        (let [[tag elem] elem*
              alias-fq (sql/qualify alias-layer field)]

          (case tag

            :cst
            (let [[tag v] elem]
              (let [where [:= alias-fq v]]
                (conj! where* where)))

            :var
            (if (vm/bound? vm elem)
              (let [where [:= alias-fq (vm/get-val vm elem)]]
                (conj! where* where))
              (vm/bind vm elem alias-fq))

            :blank nil

            ;; else
            (e/error-case! elem*))))

      (finish-layer scope
                    [alias alias-layer]
                    (persistent! where*)))))


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
    (e/error! "Wrong database: %s" var)))


(defn- process-in
  [{:as scope :keys [vm qb dm sg qp]}
   inputs params]

  (let [n-inputs (count inputs)
        n-params (count params)]
    (when-not (= n-inputs n-params)
      (e/error! "Arity mismatch: %s input(s) and %s param(s)"
              n-inputs n-params)))

  (doseq [[input-src param] (u/zip inputs params)]
    (let [[tag input] input-src]
      (case tag

        ;; todo add rules
        ;; :pattern-var
        ;; (let [rules* (group-rules input)]
        ;;   )

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
                  alias-full (h/as-fields alias-coll alias-fields)
                  from [{:values param} alias-full]]

              (qb/add-from qb from)

              (doseq [[input alias-field]
                      (u/zip input alias-fields)]
                (let [[tag input] input
                      alias-fq (sql/qualify alias-coll alias-field)]
                  (case tag
                    :unused nil
                    :var
                    (vm/bind vm input alias-fq)))))

            :bind-coll
            (let [{:keys [var]} input
                  alias-coll (sg "coll")
                  alias-field (sg "field")
                  alias-full (h/as-field alias-coll alias-field)
                  field (sql/qualify alias-coll alias-field)
                  values {:values (mapv vector param)}
                  from [values alias-full]]
              (vm/bind vm var field)
              (qb/add-from qb from))

            :bind-tuple nil ;; bug in datomic spec
            #_
            (do
              (when-not (= (count input)
                           (count param))
                (e/error! "Arity mismatch: %s != %s" input param))

              (doseq [[input param-el] (u/zip input param)]
                (let [[tag input] input]
                  (case tag
                    :var
                    (vm/bind vm input param
                              :in input-src (type param-el))))))

            :bind-scalar
            (let [value (if (h/lookup? param)
                          (resolve-lookup! scope param)
                          param)]
              (let [_a (sg (name input))
                    _p (sql/param _a)]
                (qp/add-param qp _a value)
                (vm/bind vm input _p)))))))))


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
                     (vm/get-val vm arg)

                     :cst
                     (let [[tag arg] arg]
                       (let [param (sg "param")]
                         (qp/add-param qp param arg)
                         (sql/param param)))))))

        pred-expr (into [pred] args*)]

    (case pred-tag
      :sym
      pred-expr
      ;; else
      (e/error-case! pred-tag))))


(defn- add-function
  [{:as scope :keys [qb vm sg qp]}
   expression]

  (let [{:keys [expr binding]} expression
        {:keys [fn args]} expr

        args* (for [arg args]
                (let [[tag arg] arg]
                  (case tag
                    :var
                    (vm/get-val vm arg)

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
      (vm/bind vm binding fn-expr)))

  nil)


(defn split-rule-vars
  [rule-vars]
  (let [var-map (apply hash-map rule-vars)
        {:keys [vars* vars]} var-map
        {:keys [in out]} vars*]
    (concat
     (for [var in]
       [var true])
     (for [var out]
       [var false])
     (for [var vars]
       [var false]))))


;; define that func in advance
(declare process-clauses)


;; TODO drop it
(def RULES
  #_
  (group-rules rules))


(defn- add-rule
  [scope
   expression]

  (let [{:keys [rule-name vars]} expression

        vars-dst (for [var vars]
                   (let [[tag var] var]
                     (case tag
                       :var var)))


        rule (get RULES rule-name)

        _ (when-not rule
            (e/error! "Cannot resolve a rule %s" rule-name))

        {:keys [clauses head]} rule
        {:keys [vars]} head

        vars-src-pairs (split-rule-vars vars)

        arity-dst (count vars-dst)
        arity-src (count vars-src-pairs)

        _ (when-not (= arity-src arity-dst)
            (e/error! "Arity error in rule %s" rule-name))

        vm-dst (:vm scope)
        vm-src (vm/manager)

        _ (doseq [[var-dst [var-src req?]]
                  (u/zip vars-dst vars-src-pairs)]

            (if req?

              (let [val (vm/get-val vm-dst var-dst)]
                (vm/bind vm-src var-src val))

              (when (vm/bound? vm-dst var-dst)
                (let [val (vm/get-val vm-dst var-dst)]
                  (vm/bind vm-src var-src val)))))

        scope (assoc scope :vm vm-src)

        result (process-clauses scope clauses)]

    (vm/consume vm-dst vm-src)

    (join-and result)))


(defn- add-clause
  [scope clause]
  (let [[tag expression] clause]
    (case tag

      :rule-expr
      (add-rule scope expression)

      :fn-expr
      (add-function scope expression)

      :pred-expr
      (add-predicate scope expression)

      :data-pattern
      (add-pattern scope expression)

      ;; else
      (e/error-case! clause))))


(defmacro with-lvl-up
  [scope & body]
  `(let [~scope (update ~scope :lvl inc)]
     ~@body))


(defmacro with-vm-subset
  [scope vars & body]
  `(let [~scope (update ~scope :vm vm/subset ~vars)]
     ~@body))


(defn- process-not-clause
  [scope clause]
  (vm/with-read-only
    (with-lvl-up scope
      (let [{:keys [clauses]} clause]
        [:not
         (join-and
          (process-clauses scope clauses))]))))


(defn- process-or-clause
  [scope clause]
  (with-lvl-up scope
    (let [{:keys [clauses]} clause]
      (join-or
       (for [clause clauses]
         (let [[tag clause] clause]
           (case tag

             :clause
             (join-and
              (process-clauses scope [clause]))

             :and-clause
             (with-lvl-up scope
               (let [{:keys [clauses]} clause]
                 (join-and
                  (process-clauses scope clauses)))))))))))


(defn- process-clauses
  [{:as scope :keys [qb]}
   clauses]

  (doall

   (for [clause clauses]

     (let [[tag clause] clause]

       (case tag

         :or-clause
         (process-or-clause scope clause)

         :or-join-clause
         (e/error! "or-join is not implemented")

         :not-clause
         (process-not-clause scope clause)

         :not-join-clause
         (e/error! "not-join is not implemented")

         :expression-clause
         (add-clause scope clause))))))


(defn- process-where
  [{:as scope :keys [qb]}
   clauses]
  (let [where (join-and (process-clauses scope clauses))]
    #_
    (qb/add-where qb where)))


(defn- find-elem-agg?
  [find-elem]
  (let [[tag _] find-elem]
    (= tag :agg)))


(defn- add-pull-expression
  [{:as scope :keys [qb vm pp]}
   expression]

  ;; add select
  (let [{:keys [var pattern]} expression]
    (qb/add-select qb (vm/get-val vm var))

    ;; add post-processing
    (let [index (qb/last-column-index qb)]
      (pp/add-pull pp index pattern))))


(defn- add-find-elem
  [{:as scope :keys [vm qb sg]}
   find-elem*]
  (let [[tag find-elem] find-elem*
        alias (sg "f")]

    (case tag

      :pull-expr
      (add-pull-expression scope find-elem)

      :agg
      (let [{:keys [name args]} find-elem
            call (apply sql/call name
                        (for [arg args]
                          (let [[tag arg] arg]
                            (case tag
                              :var
                              (vm/get-val vm arg)))))]
        (qb/add-select qb [call alias]))

      :var
      (let [val (vm/get-val vm find-elem)]
        (qb/add-select qb [val alias]))

      (e/error! "No matching clause: %s" find-elem*))

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

          ;; :tuple TODO
          ;; https://github.com/alexanderkiel/datomic-spec/issues/6

          (:coll :scalar)
          (let [{:keys [elem]} find-spec]
            [elem])

          :rel find-spec)

        aggs? (map find-elem-agg? find-elem-list)
        group? (some identity aggs?)]

    (doseq [[find-elem agg?] (u/zip find-elem-list aggs?)]
      (let [alias (add-find-elem scope find-elem)]
        (when (and group? (not agg?))
          (qb/add-group-by qb alias))))))


(defn- process-arrays
  [{:as scope :keys [en]}
   query]
  (rest (en/query en query {:as-arrays? true})))


(defn- process-find-type
  [result find-type]
  (case find-type
    :coll (map first result)
    :scalar (ffirst result)
    result))


(defmulti post-process-column
  (fn [scope result idx pp-map]
    (:type pp-map)))


(defmethod post-process-column :pull
  [scope result idx pp-map]

  (let [{:keys [pattern]} pp-map

        ids (for [row result]
              (get row idx))

        p (pull/pull-many scope '[*] ids)
        p* (group-by :db/id p)]

    (for [row result]
      (update row idx
              (fn [id]
                (first (get p* id)))))))


(defn- post-process
  [{:as scope :keys [pp]}
   result]
  (reduce-kv
   (fn [result idx pp-map]
     (post-process-column scope result idx pp-map))
   result
   @pp))


(defn- q-internal
  [{:as scope :keys [en am]}
   query-parsed
   & query-inputs]

  (let [scope (assoc scope
                     :lvl 0
                     :sg (u/sym-generator)
                     :vm (vm/manager)
                     :qb (qb/builder)
                     :dm (dm/manager)
                     :qp (qp/params)
                     :pp (pp/manager))

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

    (qb/debug qb @qp)

    (as-> (qb/format qb @qp) $
      (process-arrays scope $)
      (post-process scope $)
      (process-find-type $ find-type))))


(defn- parse-query
  [query-list]
  (let [result (s/conform ::ds/query query-list)]
    (if (= result ::s/invalid)
      (e/error! "Cannot parse query: %s" query-list)
      result)))


(defn q [scope query-list & args]
  (apply q-internal
         scope
         (parse-query query-list)
         args))
