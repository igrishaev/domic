
(defn transact [maps]

  (jdbc/with-db-transaction [tx db]

    (doseq [map maps]

      (let [new? true
            e 43
            t 100
            ]
        (if new?
          (jdbc/insert-multi! tx
                              :datoms4
                              (for [[key value] map]
                                {:e e :a key :v value :t t}))

          (doseq [[key value] map]

            (let [singular? true]

              (if singular?

                (jdbc/delete! tx
                              :datoms4
                              []

                              (for [[key value] map]
                                {:e e :a key :v value :t t}))))

            ))))))


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
              (vm/bind! vm elem fq-field))))))


(defn gen-data
  []

  (let [artist-ids [1 2 3 4 5]
        artist-names ["Queen" "Abba" "Beatles" "Pink Floyd" "Korn"]
        release-range (range 1 999)
        year-range (range 1970 1999)

        db {:dbtype "postgresql"
            :dbname "test"
            :host "127.0.0.1"
            :user "ivan"
            :password "ivan"}


        ]

    (doseq [artist-id artist-ids]

      (let [artist-name (get artist-names (dec artist-id))]

        (clojure.java.jdbc/insert! db :datoms4 {:e (do artist-id)
                                                :a (do :artist/name)
                                                :v (do artist-name)
                                                :t (do 42)})))
    (doseq [release-id (range 1 2000)]

      (let [release-artist (rand-nth artist-ids)
            release-year (rand-nth year-range)]

        (clojure.java.jdbc/insert!
         db :datoms4 {:e (do release-id)
                      :a (do :release/artist)
                      :v (do release-artist)
                      :t (do 42)})

        (clojure.java.jdbc/insert!
         db :datoms4 {:e (do release-id)
                      :a (do :release/year)
                      :v (do release-year)
                      :t (do 42)})))))


          ;; query (str "explain analyze " query)
          ;; pg-args (mapv en/->pg args)


;; {:db/id 42
;;  :artist/name "Old Name"
;;  :artist/album [1 2]}

;; {:db/id 42
;;  :artist/name "New Name"
;;  :artist/album [2 3]
;; }

;; {:db/id 42
;;  :artist/name "New Name"
;;  :artist/album [1 2 3]}

;; :artist/album "1"
;; :artist/album "2"

;; {:db/id 42
;;  :artist/year 1995       ;; insert
;;  :artist/name "New Name" ;; update
;;  :artist/album 2         ;; noop
;;  :artist/album 3         ;; insert
;;  }



(clojure.java.jdbc/db-query-with-resultset
 _db "select '0'"
 (fn [rs]
   (.next rs)
   (.getBoolean rs 1)
   )
 )


(defn- next-id
  [{:as scope :keys [en]}]
  (let [query ["select nextval(?) as id" seq-name]]
    (-> (en/query en query)
        first
        :id)))

(defn- maps->list
  [maps]
  (let [result* (transient [])]
    (doseq [map maps]
      (let [e (or (:db/id map)
                  (str (gensym "e")))]
        (doseq [[a v] (dissoc map :db/id)]
          (conj! result* [:db/add e a v]))))
    (persistent! result*)))


#_
(defn parse-tx-data [tx-data]
  (s/conform ::ds/tx-data tx-data))

#_
(parse-tx-data
 [[:db/add 1 :foo 42]
  [:db/retract 1 :foo 42]
  {:foo/bar 42}
  [:db/func 1 2 3 4]])

#_
[[:assertion {:op :db/add :eid 1 :attr :foo :val 42}]
 [:retraction {:op :db/retract :eid 1 :attr :foo :val 42}]
 [:map-form #:foo{:bar 42}]
 [:transact-fn-call {:fn :db/func :args [1 2 3 4]}]]


#_
(clojure.pprint/pprint
 (prepare-tx-data
  _scope
  [[:db/add 1 :foo 42]
   [:db/retract 1 :foo 42]
   {:db/id 666
    :foo/bar 42
    :foo/ggggggg "sdfsdf"
    :release/year ["a" "b" "c"]}
   [:db/func 1 2 3 4]]))

#_
{:datoms
 [[:db/add 1 :foo 42]
  [:db/retract 1 :foo 42]
  [:db/add 666 :foo/bar 42]
  [:db/add 666 :foo/ggggggg "sdfsdf"]
  [:db/add 666 :release/year "a"]
  [:db/add 666 :release/year "b"]
  [:db/add 666 :release/year "c"]],
 :tx-fns [[:db/func 1 2 3 4]]}


        (mapcat attr pull-result)
        (map attr pull-result))



(ns domic.query-builder
  (:refer-clojure :exclude [format])
  (:require
   [clojure.pprint :refer [pprint]]
   [honeysql.core :as sql]))


(defprotocol IQueryBuilder

  (debug [this] [this params])

  (set-limit [this limit])

  (set-distinct [this])

  (where-stack-up [this op])

  (where-stack-down [this])

  (add-clause [this section clause])

  (add-select [this clause])

  (add-with [this clause])

  (add-group-by [this clause])

  (add-from [this clause])

  (add-where [this clause])

  (->map [this])

  (format [this] [this params]))


(defmacro with-where [qb op & body]
  `(do
     (where-stack-up ~qb ~op)
     (try
       ~@body
       (finally
         (where-stack-down ~qb)))))

(defmacro with-where-and [qb & body]
  `(with-where ~qb :and ~@body))

(defmacro with-where-not [qb & body]
  `(with-where ~qb :not ~@body))

(defmacro with-where-or [qb & body]
  `(with-where ~qb :or ~@body))

;; (defmacro with-where-not-and [qb & body]
;;   `(with-where-not ~qb
;;      (with-where-and ~qb
;;        ~@body)))

;; (defmacro with-where-not-or [qb & body]
;;   `(with-where-not ~qb
;;      (with-where-or ~qb
;;        ~@body)))


(defn update-in*
  [data path func & args]
  (if (empty? path)
    (apply func data args)
    (apply update-in data path func args)))


(def conj* (fnil conj []))

(def WHERE-EMPTY [:and])


(defrecord QueryBuilder
    [where
     where-path
     sql]

  IQueryBuilder

  (set-limit [this limit]
    (swap! sql assoc :limit limit))

  (debug [this]
    (debug this {}))

  (debug [this params]
    (pprint (->map this))
    (println (format this params)))

  (set-distinct [this]
    (swap! sql update :modifiers conj* :distinct))

  (where-stack-up [this op]
    (let [index (count (get-in @where @where-path))]
      (swap! where update-in* @where-path conj* [op])
      (swap! where-path conj* index)))

  (where-stack-down [this]
    (swap! where-path (comp vec butlast)))

  (add-where [this clause]
    (swap! where update-in* @where-path conj* clause))

  (add-clause [this section clause]
    (swap! sql update section conj* clause))

  (add-with [this clause]
    (add-clause this :with clause))

  (add-select [this clause]
    (add-clause this :select clause))

  (add-group-by [this clause]
    (add-clause this :group-by clause))

  (add-from [this clause]
    (add-clause this :from clause))

  (->map [this]
    (let [where* @where]
      (cond-> @sql
        (not= where* WHERE-EMPTY)
        (assoc :where where*))))

  (format [this]
    (format this nil))

  (format [this params]
    (sql/format (->map this)

                :params params
                :allow-namespaced-names? true
                :quoting :ansi

)))


(defn builder
  []
  (->QueryBuilder (atom WHERE-EMPTY)
                  (atom [])
                  (atom {})))


(def builder? (partial satisfies? IQueryBuilder))


#_
(do

  (def _b (builder))

  (add-where _b [:= 1 1])
  (add-where _b [:= 2 2])
  (with-where-not _b
    (add-where _b [:= 3 3]))
  (with-where-not-and _b
    (add-where _b [:= 4 4])
    (add-where _b [:= 5 5]))
  (with-where-or _b
    (add-where _b [:= 6 6])
    (add-where _b [:= 7 7]))

  (clojure.pprint/pprint (->map _b)))


(defn mvec
  []
  (let [coll* (transient [])]
    (fn [op & args]
      (case op
        :add
        (let [[value] args]
          (conj! coll* value))
        :get
        (persistent! coll*)))))


(defn mmap
  []
  (let [coll* (transient {})]
    (fn [op & args]
      (case op
        :add
        (let [[key value] args]
          (assoc! coll* key value))
        :get
        (persistent! coll*)))))


(defn collect-foo
  [{:as scope :keys [am]}
   pull-result refs backrefs]

  (let [elist* (transient #{})
        pattern* (transient #{})]

    (doseq [[attr pattern] refs]

      (doseq [p pattern]
        (conj! pattern* p))

      (if (am/multiple? am attr)

        (doseq [pull-row pull-result]
          (doseq [{:db/keys [id]} (get pull-row attr)]
            (conj! elist* id)))

        (doseq [pull-row pull-result]
          (let [{:db/keys [id]} (get pull-row attr)]
            (conj! elist* id)))))

    (doseq [[_attr pattern] backrefs]

      (doseq [p pattern]
        (conj! pattern* p))



      )

    {:elist (persistent! elist*)
     :pattern (persistent! pattern*)}))


(into {} (for [[[kw] section] (partition 2 (partition-by #{:where :in :find} query))]
                         [kw section]
                         ))


(s/def ::or-clause
  (s/cat :src-var (s/? ::src-var)
         :op #{'or}
         :clauses (s/+ (s/or :clause ::clause :and-clause ::and-clause))))


(s/def ::rule-vars
  (s/or :vars (s/+ ::variable)
        :vars* (s/cat :in (s/spec (s/+ ::variable)) :out (s/* ::variable))))


(doseq [[var-dst [var-src req?]]
            (zip vars-dst vars-src-pairs)]

      (when-not (vm/bound? vm-dst var-dst)
        (let [val (vm/get-val vm-src var-src)]
          (vm/bind vm-dst var-dst val))))


(def query
  '
  [:find ?name ?a ?x ?y ?m ?p
   :in $ $data ?name [?x ...] ?y [[?m _ ?p]]
   :where
   [$ ?a :artist/name ?name]])


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

   [(= ?y 1999)]

   ;; [(in ?y 1985 1986 1987)]

   ;; [(+ ?y 100) ?y1]
   ;; [(- ?y 100) ?y2]
   ;; [(* ?y 100) ?y3]
   ;; [(/ ?y 100) ?y4]
   #_
   [(foo_bar ?y 1 2 3) ?y5]

   #_
   [$ ?a :db/ident :metallica]])


(def query
  '
  [:find (pull ?r [*])
   :in $ ?a
   :where
   [$ ?r :release/artist ?a]
   [$ ?r :release/year ?y]

   (not (not (not [(= ?y 1999)])))])

[?e :db/ident :metallica]


(extend-protocol jdbc/IResultSetReadColumn

  PgArray
  (result-set-read-column [pgarray metadata index]
    (let [array-type (.getBaseTypeName pgarray)
          array-java (.getArray pgarray)]
      (with-meta
        (set array-java)
        {:sql/array-type array-type}))))

org.postgresql.util.PGobject
org.postgresql.jdbc.PgArray


(defn build-back-refs
  [attr-list]
  (->> attr-list
       (filter -ref-attr?)
       (map ->back-ref)))

(defn ->back-ref
  [^Keyword attr]
  (let [ident (:db/ident attr)
        a-ns (namespace ident)
        a-name (name ident)
        ident-rev (keyword a-ns (str "_" a-name))]
    (assoc attr
           :db/doc (format "A backref to %s" ident)
           :db/ident ident-rev
           :db/cardinality :db.cardinality/many)))


(defn -ref-attr?
  [attr]
  (some-> attr :db/valueType (= :db.type/ref)))


(def pg-mapping
  {:db.type/string  :text
   :db.type/ref     :integer
   :db.type/integer :integer})


(defn pull*-refs
  [{:as scope :keys [en]}
   ids-ref attrs-ref & [attrs]]

  (let [params*    (transient {})
        params*add (adder params*)

        ids-ref*   (mapv params*add ids-ref)
        attrs-ref* (mapv params*add attrs-ref)
        attrs*     (mapv params*add attrs)

        v-cast (sql/call :cast :v :integer)

        sub (sql/build
             :select :e
             :from :datoms4
             :where [:and
                     [:in v-cast ids-ref*]
                     [:in :a attrs-ref*]])

        sql (sql/build
             :select :* :from :datoms4
             :where [:and
                     [:in :e sub]
                     (when attrs [:in :a attrs*])])

        params (persistent! params*)
        query (sql/format sql params)]

    (en/query-rs en query (rs->datoms scope))))


(clojure.pprint/pprint
            (domic.transact/prepare-tx-data
            _scope [{:release/artist 100
                              :release/year 1995
                              :release/tag ["blues" "jazz"]}
                             ]))

{:datoms
 [[:db/add "e35841" :release/artist 100]
  [:db/add "e35841" :release/year 1995]
  [:db/add "e35841" :release/tag "blues"]
  [:db/add "e35841" :release/tag "jazz"]],
 :tx-fns []}


(clojure.pprint/pprint
            (domic.transact/fffffffff
            _scope

            [[:db/add 3333 :release/artist [:foo/ssf 42]]
             [:db/add "e35841" :release/year 1995]
             [:db/add [:release/artist 999] :release/tag "dsdfs"]
             [:db/add 555 :release/tag "jazz"]]
            ))


{:db/id [:user/email "test@test.com"]
 :user/name "Hello"
 :user/age 1999
 }

[[[:user/email "test@test.com"] :user/name "sdfsdf"
  [:user/email "test@test.com"] :user/age 1999]
 ]


[[1 :user/name "sdfsdf"
  1 :user/age 1999]
 ]


{:db/id [:user/email "test@test.com"]
 :user/api-key "USHDFOIJSDFIJSFS"
 :user/age 1999
 }

                      1                          2

[[:db/add [:user/email "test@test.com"] :user/api-key "USHDFOIJSDFIJSFS"]
 [:db/add [:user/email "test@test.com"] :user/age 15342323]
]


(defn validate-tx-data
  [{:as scope :keys [am]}
   datoms]

  ;; (println datoms)

  (let [ids* (transient #{})]

    (doseq [[_ e a v] datoms]

      (when-not (am/known? am a)
        (e/error! "Unknown attribute: %s" a))

      (when (real-id? e)
        (conj! ids* e))

      (when (am/ref? am a)
        (conj! ids* v)))

    (if-let [ids (-> ids* persistent! not-empty)]

      (let [pull (p2/pull* scope ids)
            ids-found (->> pull (map :e) set)
            ids-left (set/difference ids ids-found)
            ids-count (count ids-left)]

        (cond
          (> ids-count 1)
          (e/error! "Entities %s are not found"
                    (util/join ids-left))
          (= ids-count 1)
          (e/error! "Entity %s is not found"
                    (first ids-left)))))))


(let [datoms* (doall datoms1)]

  (if (not-empty? @-cache)

            (doall
             (for [[op e a v] datoms*]
               [op (cswap e) a v]))

            datoms*))


(defn- process-not-join-clause
  [scope clause]
  (vm/with-read-only
    (with-lvl-up scope
      (let [{:keys [vars clauses]} clause]
        (with-vm-subset scope vars
          [:not
           (join-and
            (process-clauses scope clauses))])))))


(defn- process-or-join-clause
  [scope clause]

  (with-lvl-up scope
    (let [{:keys [rule-vars clauses]} clause

          vars-src-pairs (split-rule-vars rule-vars)

          vm-dst (:vm scope)
          vm-src (vm/manager)

          _ (doseq [[var req?] vars-src-pairs]

              (if req?

                (let [val (vm/get-val vm-dst var)]
                  (vm/bind vm-src var val))

                (when (vm/bound? vm-dst var)
                  (let [val (vm/get-val vm-dst var)]
                    (vm/bind vm-src var val)))))

          scope (assoc scope :vm vm-src)

          result (doall
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
                             (process-clauses scope clauses))))))))]

      (vm/consume vm-dst vm-src)

      (join-or result))))



SELECT DISTINCT

                "layer1"."e" AS "f1",
CAST("layer3"."v" AS bigint) AS "f2"

FROM "datoms4" "layer1"

INNER JOIN "datoms4" "layer2" ON ("layer2"."a" = 'release/artist' AND CAST("layer2"."v" AS bigint) = "layer1"."e")
INNER JOIN "datoms4" "layer3" ON ("layer3"."e" = "layer2"."e" AND "layer3"."a" = 'release/year')

WHERE (("layer1"."a" = 'artist/name' AND "layer1"."v" = 'Queen'))
;


explain analyze
SELECT DISTINCT
                 "layer1"."e" AS "f1",
CAST("layer3"."v" AS integer) AS "f2"

FROM
"datoms4" "layer1",
"datoms4" "layer2",
"datoms4" "layer3"

WHERE

"layer1"."a" = 'artist/name' AND "layer1"."v" = 'Queen'
and
"layer2"."a" = 'release/artist' AND CAST("layer2"."v" AS integer) = "layer1"."e"
and
"layer3"."e" = "layer2"."e" AND "layer3"."a" = 'release/year'
;





SELECT DISTINCT
                 "layer1"."e" AS "f1",
CAST("layer3"."v" AS integer) AS "f2"

FROM
"datoms4" "layer1",
"datoms4" "layer2",
"datoms4" "layer3"


WHERE

"layer1"."a" = 'artist/name' AND "layer1"."v" = 'Queen'
and
"layer2"."a" = 'release/artist' AND CAST("layer2"."v" AS integer) = "layer1"."e"
and
"layer3"."e" = "layer2"."e" AND "layer3"."a" = 'release/year'
;


 1978
 1971
 1982
 1983
 1994
 1980
 1981
 1988
 1986
 1972
 1996
 1987
 1992
 1976
 1993
 1974
 1985
 1997
 1990
 2000
 1973
 1991
 2999
 1979
 1989
 1998
 1984
 1995
 1975
 1999
 1977
 333
 1970







(q _scope '[:find ?a ?name ?y
                       :in $
                       :where
                       [$ ?a :artist/name ?name]
                       [$ ?r :release/artist ?a]
                       [$ ?r :release/year ?y]
                       ]
              (domic.db/pg)

              )



(q _scope '[:find ?r ?y ?name
                       :in $
                       :where
                       [$ ?r :release/artist ?a]
                       [$ ?r :release/year ?y]
                       [$ ?a :artist/name ?name]

                       ]
              (domic.db/pg)

              )



(q _scope '[:find ?y ?name
                       :in $
                       :where
                       [$ ?a :artist/name ?name]
                       [$ ?r :release/artist ?a]
                       [$ ?r :release/year ?y]
                       (or [(= ?name "Test")]
                           [(= ?name "G G G")])

                       ]
              (domic.db/pg)
              )




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



[{:head {:name queen?, :vars-req [?a], :vars-opt [?foo ?bar]},
  :clauses
  [[:data-pattern
    {:elems
     [[:var ?a] [:cst [:kw :artist/name]] [:cst [:str "Queen"]]]}]]}]



(q _scope '[:find ?a ?name
                       :in $ %
                       :where
                       [$ ?a :artist/name ?name]
                       (queen? ?a)]
              (domic.db/pg)
              _rules
              )


-                  (if *nested?*
-                    (conj! wheres* [:= alias-sub-field val])
-                    (qb/add-where qb [:= alias-sub-field val]))


-      (when *nested?*
-        (join-and (persistent! wheres*)))))


(q _scope '[:find ?r ?y
                       :in $ %
                       :where
                       [$ ?r :release/year]
                       (release-of-year? ?r ?y)
                       [(= ?y 1223)]

                       ]

              (domic.db/pg)
              _rules
              )



[{:db/ident       :artist/name
  :db/valueType   :db.type/string
  :db/cardinality :db.cardinality/one}

 {:db/ident       :release/artist
  :db/valueType   :db.type/ref
  :db/cardinality :db.cardinality/one
  :db/isComponent true}

 {:db/ident       :release/year
  :db/valueType   :db.type/long
  :db/cardinality :db.cardinality/one}

 {:db/ident       :release/tag
  :db/valueType   :db.type/string
  :db/cardinality :db.cardinality/many}]



(q _scope '[:find ?name
                       :in $
                       :where
                       [$ _ :artist/name ?name]

                       ]

              (domic.source/table :__test_datoms)
              )



(q _scope '[:find ?name
:in $
:where
[$ ?a ?b ?c]
]

[[1 2 3]
[4 5 6]
[7 8 9]]

)


(init-db [db {:keys [qb]}]
           (let [{:keys [alias fields data]} db
                 with [[alias {:columns fields}] {:values data}]]
(qb/add-with qb with)))



(q _scope '[:find ?a ?b ?c ?name
            :in $data
            :where
            [$data ?a ?b ?c]
            [$ ?a :artist/name ?name]
            ]
     [[49 1 2]
]

)





;; tuple
           (q _scope '[:find ?attr
                       :in [?a _ ?name]
                       :where
                       [?a ?attr ?name]
                       ]
              [199 "foo" "Queen"]
              )

;; scalar
           (q _scope '[:find ?attr
                       :in ?a
                       :where
                       [?a ?attr ?name]
                       ]
              199
              )


(let [value (if (h/lookup? param)
                (resolve-lookup! scope param)
                param)]
    (let [_a (sg (name input))
          _p (sql/param _a)]
      (qp/add-param qp _a value)
      (vm/bind vm input _p)))



;; coll
           (q _scope '[:find ?name
                       :in [?a ...]
                       :where
                       [?a :artist/name ?name]
                       ]
              [49 199]
              )
