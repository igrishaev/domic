
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



(clojure.java.jdbc/db-query-with-resultset _db "select '0'"
                                             (fn [rs]
                                               (.next rs)
                                               (.getBoolean rs 1)
                                               )
                                             )
