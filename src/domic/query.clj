(ns domic.query

  (:require
   [clojure.spec.alpha :as s]

   [domic.runtime :as rt]
   [domic.const :as const]
   [domic.pull :as pull]
   [domic.sql-helpers :as h]
   [domic.rule-manager :as rm]
   [domic.error :as e]
   [domic.var-manager :as vm]
   [domic.query-builder :as qb]
   [domic.pp-manager :as pp]
   [domic.util :as u]
   [domic.source :as src]
   [domic.source-manager :as sm]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]
   [domic.spec-datomic :as sd]

   [honeysql.core :as sql])

  (:import
   [domic.source Table Dataset]))


;; todo

;; fix find tupe

;; process with
;; process maps
;; deal with pull pattern
;; detect idents only for e and v/ref

;; get-some
;; ground
;; fulltext
;; tuple
;; tx-ids
;; tx-data
;; untuple

;; rest of aggregates


(declare process-clauses)


(defn group-rules
  [rules]
  (let [rules* (s/conform ::sd/rules rules)]
    (when (= rules* ::s/invalid)
      (e/error! "Wrong rules: %s" rules))
    (into {} (for [rule* rules*]
               [(-> rule* :head :name) rule*]))))


(defn- join-op
  [op clauses]

  (case op
    (not :not)
    [:not (join-op :and clauses)]
    ;; else
    (when-let [clauses* (not-empty (filter some? clauses))]
      (into [op] clauses*))))


(def join-and (partial join-op :and))
(def join-or  (partial join-op :or))


(defn- add-predicate-missing
  [{:as scope :keys [table
                     qb vm qp]}
   expression]

  (let [{:keys [expr binding]} expression
        {:keys [args]} expr

        [bind-tag binding] binding

        [arg-src arg-e arg-a] args

        e (let [[tag e] arg-e]
            (case tag :var e))

        a (let [[tag a] arg-a]
            (case tag :cst (let [[tag a] a]
                             (case tag :kw a))))

        e-var   (vm/get-val vm e)
        a-param (qp/add-alias qp a)]

    [:not (sql/call :exists
                    (sql/build
                     :select 1
                     :from table
                     :where [:and
                             [:= :e e-var]
                             [:= :a a-param]]
                     :limit 1))]))


(defn- add-predicate-common
  [{:as scope :keys [qb vm sg qp]}
   expression]

  (let [{:keys [expr]} expression
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
                         (sql/param param)))))))]

    (into [pred] args*)))


(defn- add-predicate
  [scope expression]

  (let [{:keys [expr]} expression
        {:keys [pred]} expr

        [pred-tag pred] pred]

    (case pred-tag
      :sym

      (case pred

        missing?
        (add-predicate-missing scope expression)

        ;; else
        (add-predicate-common scope expression))

      (e/error-case! pred-tag))))


(defn- process-bool-expr
  [scope
   expression]

  (let [{:keys [op clauses]} expression]
    (join-op op (doall (for [[tag expression] clauses]
                         (case tag

                           :pred-expr
                           (add-predicate scope expression)

                           :bool-expr
                           (process-bool-expr scope expression)))))))


(defn -db-check-fn-arity
  [num op args]
  (when-not (some-> args count (= num))
    (e/error! "Func/operator %s takes one argument, %s given"
              op (u/join args))))


(defn- ->db-func-unary-op-pre
  [op args]
  (-db-check-fn-arity 1 op args)

  (let [[arg1] args]
    (sql/raw [(format "%s " op) arg1])))


(defn- ->db-func-unary-op-post
  [op args]
  (-db-check-fn-arity 1 op args)

  (let [[arg1] args]
    (sql/raw [arg1 (format " %s" op)])))


(defn- ->db-func-binary-op
  [op args]
  (-db-check-fn-arity 2 op args)

  (let [[arg1 arg2] args]
    (sql/raw [arg1 (format " %s " op) arg2])))


(defn- ->db-func-expression
  [fn args]

  (let [fn-expr
        (case fn

          (+ - * /)
          (->db-func-binary-op fn args)

          mod
          (->db-func-binary-op (symbol "%") args)

          exp
          (->db-func-binary-op (symbol "^") args)

          fact
          (->db-func-unary-op-post "!" args)

          sqrt
          (->db-func-unary-op-pre "|/" args)

          abs
          (->db-func-unary-op-pre "@" args)

          ;; else
          (apply sql/call fn args))]

    (sql/raw ["(" fn-expr ")"])))


(defn- add-function-get-else
  [{:as scope :keys [table
                     qb vm qp]}
   expression]

  (let [{:keys [expr binding]} expression
        {:keys [args]} expr

        [bind-tag binding] binding

        [arg-src arg-e arg-a arg-d] args

        e (let [[tag e] arg-e]
            (case tag :var e))

        a (let [[tag a] arg-a]
            (case tag :cst (let [[tag a] a]
                             (case tag :kw a))))

        d (let [[tag d] arg-d]
            (case tag :cst (let [[tag d] d]
                             (case tag :str d))))

        e-var   (vm/get-val vm e)
        a-param (qp/add-alias qp a)
        d-param (qp/add-alias qp d)

        sql (sql/build

             :select :v
             :from

             [[(sql/build
                :union

                [(sql/build
                  :select [[1 :n] :v]
                  :from [table]
                  :where [:and
                          [:= :e e-var]
                          [:= :a a-param]])

                 (sql/build
                  :select [[2 :n] d-param]
                  :from [table]
                  :where [:= :e e-var])]

                :order-by :n
                :limit 1) :foo]])]

    (case bind-tag
      :bind-scalar
      (vm/bind vm binding sql)))

  nil)


(defn- add-function-common
  [{:as scope :keys [qb vm sg qp]}
   expression]

  (let [{:keys [expr binding]} expression
        {:keys [fn args]} expr

        [fn-tag fn] fn
        [bind-tag binding] binding

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


        fn-expr
        (case fn-tag
          :sym
          (->db-func-expression fn args*))]

    (case bind-tag
      :bind-scalar
      (vm/bind vm binding fn-expr)))

  nil)


(defn- add-function
  [scope expression]

  (let [{:keys [expr]} expression
        {:keys [fn args]} expr

        [fn-tag fn] fn]

    (case fn-tag
      :sym
      (case fn

        get-else
        (add-function-get-else scope expression)

        ;; else
        (add-function-common scope expression)))))


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


(defn with-source
  [obj source]
  (let [obj* (if (keyword? obj)
               (sql/inline obj) obj)]
    (with-meta obj* {:src source})))


(defprotocol ISourceActions

  (add-pattern-db [src scope expression]))


(extend-protocol ISourceActions

  Table

  (add-pattern-db [src scope expression]

    (let [{:keys [table
                  qb sg vm qp am]} scope

          add-param (partial qp/add-alias qp)

          alias (src/get-alias src)
          fields (src/get-fields src)

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

              ->cast (fn [sql]
                       (if (and v? attr)
                         (h/->cast sql pg-type)
                         sql))

              alias-sub-field (-> (sql/qualify alias-sub field)
                                  (->cast)
                                  (with-source alias-sub))

              alias-fq (-> (sql/qualify alias-table field)
                           (->cast))]

          (case tag

            :cst
            (let [[tag v] elem]

              ;; Special case: when a value is a keyword,
              ;; treat it like an ident (find by :db/ident).
              (if (and v? (= tag :kw))

                (let [e* (rt/resolve-lookup! scope [:db/ident v])
                      where [:= alias-fq e*]]
                  (qb/add-where qb-sub where))

                ;; Act as usual
                (let [param (add-param v)
                      where [:= alias-fq param]]
                  (qb/add-where qb-sub where))))

            :var
            (if (vm/bound? vm elem)

              (let [val (vm/get-val vm elem)]

                (qb/add-where qb-sub [:= alias-fq val])

                (when-let [src (-> val meta :src)]
                  (qb/add-where qb [:= alias-sub-field val])
                  (qb/add-from? qb-sub src)
                  (qb/add-from? qb alias-sub)
                  (qb/add-from? qb src)))

              (vm/bind vm elem alias-sub-field))

            :blank nil

            ;; else
            (e/error-case! elem*))))

      (qb/add-with qb [alias-sub (qb/->map qb-sub)])
      (qb/add-from? qb alias-sub)

      nil))

  Dataset

  (add-pattern-db [src scope expression]

    (let [{:keys [qb sg vm]} scope

          alias-main (sg "d")

          {:keys [elems]} expression
          alias-sub (src/get-alias src)
          fields (src/get-fields src)]

      (qb/add-from? qb [alias-sub alias-main])

      (doseq [[elem* field] (u/zip elems fields)]
        (let [[tag elem] elem*

              alias-fq (-> (sql/qualify alias-main field)
                           (with-source [alias-sub alias-main]))]

          (case tag

            :cst
            (let [[tag v] elem]
              (let [where [:= alias-fq v]]
                (qb/add-where qb where)))

            :var
            (if (vm/bound? vm elem)

              (let [val (vm/get-val vm elem)
                    where [:= alias-fq val]]
                (qb/add-where qb where))

              (vm/bind vm elem alias-fq))

            :blank nil

            ;; else
            (e/error-case! elem*))))

      nil)))


(defn- add-pattern
  [{:as scope :keys [sm]} expression]
  (let [{:keys [src-var]} expression
        src (sm/get-source sm (or src-var const/src-default))]
    (add-pattern-db src scope expression)))


(defn- ->dataset
  [{:as scope :keys [sg]}
   values]
  (let [[row] values
        alias (sg "data")
        fields (for [_ row] (sg "f"))]
    (src/dataset alias fields)))


(defn- resolve*
  [scope param]
  (cond
    ;; resolve if lookup
    (h/lookup? param)
    (rt/resolve-lookup! scope param)

    ;; resolve ident
    (h/ident-id? param)
    (rt/resolve-lookup! scope (h/ident->lookup param))

    :else param))


(defn- bind*
  "
  Common bind function which takes lookups and idents into account.
  "
  [{:as scope :keys [vm qp]}
   input param]
  (let [param* (resolve* scope param)
        p (qp/add-alias qp param*)]
    (vm/bind vm input p)
    p))


(defn- add-dataset
  [{:as scope :keys [qb qp]}
   dataset values]
  (let [alias (src/get-alias dataset)
        fields (src/get-fields dataset)

        fn-value (fn [value]
                   (let [value* (resolve* scope value)]
                     (qp/add-alias qp value*)))

        values* (mapv #(mapv fn-value %) values)

        with [[alias {:columns fields}] {:values values*}]]
    (qb/add-with qb with)))


(defn- process-rules-var
  [{:as scope :keys [rm]}
   rules]
  (let [rule-map (group-rules rules)]
    (rm/set-rules rm rule-map)))


(defn- process-src-var
  [{:as scope :keys [sm]}
   src-var source]
  (cond
    (src/table? source)
    (sm/add-source sm src-var source)

    (map? source)
    (e/error! "Maps datasets are not supported, use vectors instead")

    (coll? source)
    (let [dataset (->dataset scope source)]
      (sm/add-source sm src-var dataset)
      (add-dataset scope dataset source))

    :else
    (e/error-case! source)))


(defn- process-binding-rel
  [{:as scope :keys [sg qb vm]}
   input values]

  (let [[input] input
        dataset (->dataset scope values)]

    (add-dataset scope dataset values)

    (let [alias-sub (src/get-alias dataset)
          fields (src/get-fields dataset)]

      (doseq [[[tag var] field] (u/zip input fields)]

        (case tag
          :unused nil
          :var
          (let [field-fq (-> (sql/qualify alias-sub field)
                             (with-source alias-sub))]
            (vm/bind vm var field-fq)))))

    nil))


(defn- process-binding-coll
  [{:as scope :keys [sg qb vm]}
   input values]

  (let [values* (mapv vector values)
        dataset (->dataset scope values*)]

    (add-dataset scope dataset values*)

    (let [{:keys [var]} input
          vars [var]
          alias-sub (src/get-alias dataset)
          fields (src/get-fields dataset)]

      (doseq [[var field] (u/zip vars fields)]

        (let [field-fq (-> (sql/qualify alias-sub field)
                           (with-source alias-sub))]

          (vm/bind vm var field-fq))))

    nil))


(defn- process-binding-tuple
  [scope
   input param]

  (when-not (= (count input)
               (count param))
    (e/error! "Tuple arity mismatch: %s != %s" input param))

  (doseq [[input param] (u/zip input param)]
    (let [[tag input] input]
      (case tag
        :unused nil
        :var
        (bind* scope input param)))))


(defn- process-binding-scalar
  [scope input param]
  (bind* scope input param))


(defn- process-binding-var
  [scope
   input* param]

  (let [[tag input] input*]

    (case tag

      :bind-rel
      (process-binding-rel scope input param)

      :bind-coll
      (process-binding-coll scope input param)

      :bind-tuple
      (process-binding-tuple scope input param)

      :bind-scalar
      (process-binding-scalar scope input param)

      ;; else
      (e/error-case! input*))))


(defn- process-in
  [{:as scope :keys [vm qb dm sg qp rm sm]}
   inputs params]

  (let [n-inputs (count inputs)
        n-params (count params)]

    (when-not (= n-inputs n-params)
      (e/error! "IN arity mismatch: %s input(s) and %s param(s)"
                n-inputs n-params)))

  (doseq [[input* param] (u/zip inputs params)]
    (let [[tag input] input*]

      (case tag

        :rules-var
        (process-rules-var scope param)

        :src-var
        (process-src-var scope input param)

        :binding
        (process-binding-var scope input param)

        ;; else
        (e/error-case! input*)))))


(defn- split-rule-vars
  [vars-req vars-opt]
  (concat (for [var vars-req]
            [var true])
          (for [var vars-opt]
            [var false])))


(defn- add-rule
  [{:as scope :keys [rm]}
   expression]

  (let [{:keys [rule-name
                vars]} expression

        vars-dst (vec
                  (for [var vars]
                    (let [[tag var] var]
                      (case tag
                        :var var))))

        rule (rm/get-rule rm rule-name)

        {:keys [clauses head]} rule
        {:keys [vars-req
                vars-opt]} head

        vars-src-pairs (split-rule-vars vars-req vars-opt)

        arity-dst (count vars-dst)
        arity-src (count vars-src-pairs)

        _ (when-not (= arity-src arity-dst)
            (e/error! "Arity mismatch in rule %s: %s <> %s"
                      rule-name vars-dst (mapv first vars-src-pairs)))

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

    nil))


(defn- process-clauses
  [{:as scope :keys [qb]}
   clauses]

  (doseq [clause clauses]

    (let [[tag expression] clause]

      (case tag

        :data-pattern
        (add-pattern scope expression)

        :bool-expr
        (let [where (process-bool-expr scope expression)]
          (qb/add-where qb where))

        :rule-expr
        (add-rule scope expression)

        :fn-expr
        (add-function scope expression)

        :pred-expr
        (let [where (add-predicate scope expression)]
          (qb/add-where qb where))

        ;; else
        (e/error-case! clause)))))


(defn- process-where
  [{:as scope :keys [qb]}
   clauses]
  (process-clauses scope clauses))


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


(let [var? (partial s/valid? (s/tuple #{:var} ::sd/variable))]

  (defn- fix-missing-vars
    [{:as scope :keys [qb vm]}
     items]
    (clojure.walk/prewalk
     (fn [x]
       (if (var? x)
         (let [[_ var] x
               val (vm/get-val vm var)]
           (when-let [src (-> val meta :src)]
             (qb/add-from? qb src))
           nil)
         x))
     items)))


(defn- process-find
  [{:as scope :keys [vm qb]}
   find-spec with-vars]

  (let [[tag find-spec] find-spec

        find-elem-list (concat

                        (for [var with-vars]
                          [:var var])

                        (case tag

                          (:rel :tuple)
                          find-spec

                          (:coll :scalar)
                          (let [{:keys [elem]} find-spec]
                            [elem])))

        _ (fix-missing-vars scope find-elem-list)

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
    :rel    result
    :tuple  (first result)
    :coll   (mapv first result)
    :scalar (ffirst result)))


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


(defn- process-keys
  [scope result keys-expression find-type]

  (let [{:keys [keys-kw keys]} keys-expression

        fn-map (case keys-kw
                 :keys keyword
                 :strs str
                 :syms identity)

        keys* (mapv fn-map keys)

        ->map (fn [row] (into {} (u/zip keys* row)))

        -check-arity!
        (fn [row]
          (when-not (= (count row)
                       (count keys*))
            (e/error! "Find/keys arity mismatch")))]

    (case find-type

      :rel
      (do
        (-check-arity! (first result))
        (mapv ->map result))

      :tuple
      (do
        (-check-arity! result)
        (->map result))

      :coll
      (do
        (-check-arity! '[_])
        (mapv #(->map [%]) result))

      :scalar
      (do
        (-check-arity! '[_])
        (->map [result])))))


(defn- process-reduce-with
  [scope result with-vars]
  (let [n (count with-vars)]
    (mapv #(subvec % n) result)))


(defn- q-internal
  [{:as scope :keys [debug?
                     table
                     en am]}
   query-parsed
   & query-inputs]

  (let [scope (assoc scope
                     :sg (u/sym-generator)
                     :vm (vm/manager)
                     :qb (qb/builder)
                     :rm (rm/manager)
                     :sm (sm/manager)
                     :qp (qp/params)
                     :pp (pp/manager))

        {:keys [qb qp sm]} scope

        {:keys [find in where keys with]} query-parsed
        {:keys [inputs]} in

        {find-spec :spec} find
        {where-clauses :clauses} where
        {with-vars :vars} with

        find-type (get-find-type find-spec)]

    (sm/add-source sm const/src-default (src/table table))

    (case find-type
      (:scalar :tuple)
      (qb/set-limit qb (sql/inline 1))
      nil)

    (process-in scope inputs query-inputs)
    (process-where scope where-clauses)
    (process-find scope find-spec with-vars)

    (qb/set-distinct qb)

    (when debug?
      (qb/debug qb @qp)
      (qb/pprint qb @qp))

    (cond-> (qb/format qb @qp)

      true
      (as-> query (process-arrays scope query))

      with-vars
      (as-> $ (process-reduce-with scope $ with-vars))

      true
      (as-> $ (process-find-type $ find-type))

      keys
      (as-> $ (process-keys scope $ keys find-type)))))


;; (post-process scope $)

(defn- parse-query
  [query-list]
  (let [result (s/conform ::sd/query query-list)]
    (if (= result ::s/invalid)
      (e/error! "Cannot parse query: %s" query-list)
      result)))


(defn q [scope query-list & args]
  (apply q-internal
         scope
         (parse-query query-list)
         args))
