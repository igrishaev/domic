(ns domic.core2
  (:require [datomic-spec.core :as ds]

            [clojure.spec.alpha :as s]
            [honeysql.core :as sql]

            [domic.var-manager :as vm]
            [domic.sql-builder :as sb]
            [domic.attr-manager :as am]

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

  (let [attrs [{:db/ident       :artist/name
                :db/valueType   :db.type/string
                :db/cardinality :db.cardinality/one}

               {:db/ident       :release/artist
                :db/valueType   :db.type/ref
                :db/cardinality :db.cardinality/one}]

        am (am/manager attrs)

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

                    prefix (str (gensym "d"))

                    attr
                    (let [[tag a] a]
                      (case tag
                        :cst
                        (let [[tag a] a]
                          (case tag
                            :kw a))))

                    pg-type (am/get-pg-type am attr)]

                (sb/add-from sb [:datoms (keyword prefix)])

                ;; E
                (let [[tag e] e]
                  (case tag
                    :var
                    (let [sql (keyword (format "%s.e" prefix))]
                      (if (vm/bound? vm e)
                        (let [where [:= sql (vm/get-val vm e)]]
                          (sb/add-where sb where))
                        (vm/bind vm e :where sql)))))

                ;; A
                #_
                (let [[tag a] a]
                  (case tag
                    :cst
                    (let [[tag a] a]
                      (case tag
                        :kw
                        (let [where [:= (sql/raw (format "%s.a" prefix)) (kw->str a)]]
                          (sb/add-where sb where))))))

                (let [where [:= (sql/raw (format "%s.a" prefix)) (kw->str attr)]]
                  (sb/add-where sb where))

                ;; V
                (let [[tag v] v]

                  (let [sql (sql/raw (format "%s.v::%s" prefix pg-type))]

                    (case tag
                      :cst
                      (let [[tag v] v]
                        (let [where [:= sql v]]
                          (sb/add-where sb where)))

                      :var
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

    (sb/format sb)))
