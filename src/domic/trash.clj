
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
