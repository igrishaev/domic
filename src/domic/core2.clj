(ns domic.core2
  (:require [datomic-spec.core :as ds]

            [clojure.spec.alpha :as s]
            [honeysql.core :as sql]

            [domic.var-manager :as vm]
            [domic.sql-builder :as sb]

            ))


(def q
  '
  [:find ?e ?r
   :in $ ?name
   :where
   [?e :artist/name ?name]
   [?r :release/artist ?e]
   ;; [?r :release/year ?year]
   ;; (> ?year 1970)

   ]

  )



(def parsed
  '
  {:find {:find-kw :find, :spec [:rel [[:var ?e] [:var ?r]]]},
   :where
   {:where-kw :where,
    :clauses
    [[:expression-clause
      [:data-pattern
       {:elems
        [[:var ?e] [:cst [:kw :artist/name]] [:cst [:str "Queen"]]]}]]
     [:expression-clause
      [:data-pattern
       {:elems [[:var ?r] [:cst [:kw :release/artist]] [:var ?e]]}]]]}})



(defn kw->str [kw]
  (-> kw str (subs 1)))


(defn aaa
  [query-parsed]

  (let [attrs {:artist/name
               {:db/ident       :artist/name
                :db/valueType   :db.type/string
                :db/cardinality :db.cardinality/one}

               :release/artist
               {:db/ident       :release/artist
                :db/valueType   :db.type/ref
                :db/cardinality :db.cardinality/one}}

        vm (vm/manager)

        sb (sb/builder)

        {:keys [find in where]} query-parsed

        {:keys [spec]} find
        {:keys [clauses]} where]

    (doseq [clause-wrapper clauses]

      (let [[tag clause] clause-wrapper]
        (case tag
          :expression-clause
          (let [[tag expression] clause]
            (case tag
              :data-pattern
              (let [{:keys [elems]} expression
                    [e a v t] elems

                    prefix (str (gensym "d"))]

                (sb/add-from sb [:datoms (keyword prefix)])

                (let [[tag e] e]
                  (case tag
                    :var
                    (let [sql (keyword (format "%s.e" prefix))]
                      (if (vm/bound? vm e)
                        (let [where [:= sql (vm/get-val vm e)]]
                          (sb/add-where sb where))
                        (vm/bind vm e :where sql)))))

                (let [[tag a] a]
                  (case tag
                    :cst
                    (let [[tag a] a]
                      (case tag
                        :kw
                        (let [where [:= (sql/raw (format "%s.a" prefix)) (kw->str a)]]
                          (sb/add-where sb where))))))

                (let [[tag v] v]
                  (case tag
                    :cst
                    (let [[tag v] v]
                      (let [sql (sql/raw (format "%s.v" prefix))
                            where [:= sql v]]
                        (sb/add-where sb where)))

                    :var
                    (let [sql (sql/raw (format "%s.v" prefix))]
                      (if (vm/bound? vm v)
                        (let [where [:= sql (vm/get-val vm v)]]
                          (sb/add-where sb where))
                        (vm/bind vm v :where sql)))))))))))


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
                      (sb/add-select sb val))
                    (throw (new Exception "var is not bound"))))))))))
    #_
    (doseq [where-clause where-clauses]

      (let [[clause-type clause] where-clause]

        (case clause-type

          :expression-clause

          (let [[expression-type expression] clause]

            (case expression-type

              :data-pattern

              (let [{:keys [src-var elems]} expression
                    [e a v t] elems

                    [e-type e-expr] e
                    [a-type a-expr] a
                    [v-type v-expr] v

                    ]

                (case a-type
                  :cst
                  (let [[a-val-type a-val] a-expr]
                    (case a-val-type
                      :kw

                      (let [pg-type (get-attr-coercion a-val)

                            sql-prefix
                            (case e-type
                              :var
                              (let [e-var e-expr]

                                (if (vm/bound? vm e-var)

                                  (/ 0 0)

                                  (let [pass (vm/gen-prefix vm)

                                        prefix (bind-var e-var)
                                        from [:entities (keyword prefix)]
                                        var-sql (format "%s.id" prefix)]

                                    (vm/bind vm e-var :where (sql/raw "aaa"))



                                    (sql-add-from from)
                                    (var-set-sql e-var var-sql)
                                    prefix)

                                  )))

                            sql (format "(%s.data->>'%s')::%s"
                                        sql-prefix
                                        (-> a-val str (subs 1))
                                        pg-type)]

                        (case v-type

                          :var
                          (let [v-var v-expr]

                            (if (var-bound? v-var)

                              (let [where (sql/raw (format "%s = %s" sql (var-get-sql v-var)))]
                                (sql-add-where where))

                              (let [_ (bind-var v-var)
                                    where (sql/raw (format "%s is not null" sql))]
                                (sql-add-where where)
                                (var-set-sql v-var sql))))


                          :cst
                          (let [[_ v-val] v-expr]
                            (sql-add-where
                             [:= (sql/raw sql) v-val])


                            )

                          )

                        )


                      )))


                ))))))

    #_
    (let [[find-type _] find-clauses]

      (case :rel

        (let [[_ find-rels] find-clauses]

          (doseq [find-rel find-rels]

            (let [[rel-type _] find-rel]

              (case rel-type

                :var
                (let [[_ var] find-rel]

                  (if (var-bound? var)

                    (let [sql (var-get-sql var)]
                      (sql-add-select (sql/raw sql)))

                    (throw (ex-info "var is not bound"
                                    {:var var}))))))))))

    (sb/map sb)))
