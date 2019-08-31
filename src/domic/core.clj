(ns domic.core
  (:require [datomic-spec.core :as ds]
            [honeysql.core :as sql]
            [clojure.spec.alpha :as s]))


(def q
  '
  [:find ?e ?r
   :where
   [?e :artist/name "Queen"]
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

(def sqlmap {:select [:a :b :c]
             :from [:foo]
             :where [:= :f.a "baz"]})


(def sqlmap2
  {:select [:e1.id
            :e2.id
            (sql/raw (format "(e1.data->'%s')::integer" "artist/name"))]

   :from [[:entities :e1]
          [:entities :e2]]

   :where [:and
           [:> :e1.id 42]
           [:= :e1.id (sql/raw (format "(e1.data->'%s')::integer" "release/artist"))]]})


(defn aaa
  [query-parsed ]

  (let [attrs {:artist/name
               {:db/ident       :artist/name
                :db/valueType   :db.type/string
                :db/cardinality :db.cardinality/one}

               :release/artist
               {:db/ident       :release/artist
                :db/valueType   :db.type/ref
                :db/cardinality :db.cardinality/one}}

        get-attr-coercion (fn [attr]
                            (let [vtype (-> attrs attr :db/valueType)]
                              (case vtype
                                :db.type/string "text"
                                :db.type/ref    "integer")))

        query-map (atom {:select []
                         :from []
                         :where [:and]})

        sql-add-from (fn [from]
                       (swap! query-map update :from conj from))

        sql-add-where (fn [where]
                        (swap! query-map update :where conj where))


        sql-add-select (fn [select]
                         (swap! query-map update :select conj select))

        vars (atom {})
        var-bound? (fn [var]
                     (some-> vars deref (get var) :bound?))

        bind-var (fn [var]
                   (let [prefix (str (gensym "e"))
                         data {:bound? true
                               :prefix prefix}]
                     (swap! vars assoc var data)
                     prefix))

        var-set-sql (fn [var sql]
                      (swap! vars assoc-in [var :sql] sql))

        var-get-sql (fn [var]
                      (some-> vars deref (get var) :sql))

        var-prefix (fn [var]
                     (some-> vars deref (get var) :prefix))

        where-clauses (-> query-parsed :where :clauses)
        find-clauses (-> query-parsed :find :spec)
        ]

    (doseq [where-clause where-clauses]

      (let [sql-expr (atom "")

            [clause-type clause] where-clause]

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
                                (if (var-bound? e-var)

                                  (/ 0 0)

                                  (let [prefix (bind-var e-var)
                                        from [:entities (keyword prefix)]
                                        var-sql (format "%s.id" prefix)]
                                    (sql-add-from from)
                                    (var-set-sql e-var var-sql)
                                    prefix))))

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

    @query-map))
