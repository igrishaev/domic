
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
