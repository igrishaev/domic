(ns domic.transact-test
  (:require
   [clojure.test
    :refer [is deftest testing use-fixtures
            run-tests]]

   [domic.utils-test :refer [with-thrown?]]
   [domic.fixtures :as fix :refer [*scope*]]
   [domic.engine :as en]
   [domic.runtime :as rt]
   [domic.api :as api])
  (:import
   java.net.URI))


;; todo
;; custom pull


(use-fixtures :once fix/fix-with-scope)
(use-fixtures :each fix/fix-raw-insert)


(defn pull*
  [attr]

  (let [{:keys [en table]} *scope*
        query [(format "select * from %s where e = (select e from %s where a = ? limit 1)"
                       (name table) (name table)) attr]
        rows (en/query en query)]

    (->> rows (map (juxt :a :v)) sort)))


(defn tr [tx-maps]
  (api/transact *scope* tx-maps))


(defn resolve* [lookup]
  (rt/resolve-lookup! *scope* lookup))


(deftest test-unknown-attr
  (with-thrown? #"Unknown attr"
    (tr [{:artinst/name "TEST"}])))


(deftest test-simple-insert
  (tr [{:band/name "Queen"
        :band/country :country/england
        :band/website "http://queenonline.com/"}])

  (let [{:keys [en table]} *scope*

        query [(format "select e from %s where a = ?"
                       (name table)) :band/name]

        e (-> (en/query en query) first :e)

        p (api/pull *scope* '[*] e)]

    (is (= p {:db/id e
              :band/name "Queen"
              :band/country #:db{:id 100000}
              :band/website (new URI "http://queenonline.com/")}))))


(deftest test-simple-insert-with-temp-id-deref

  (tr [{:db/id "bmay"
        :person/full-name "Brian May"}

       {:band/name "Queen"
        :band/members ["bmay"]}])

  (let [{:keys [en table]} *scope*

        query [(format "select e from %s where a = ?"
                       (name table)) :band/name]
        e1 (-> (en/query en query) first :e)
        p (api/pull *scope* '[* {:band/members [*]}] e1)

        query [(format "select e from %s where a = ?"
                       (name table)) :person/full-name]
        e2 (-> (en/query en query) first :e)]

    (is (= p {:db/id e1
              :band/name "Queen"
              :band/members [{:person/full-name "Brian May", :db/id e2}]}))))


(deftest test-lookups-and-idents-resolved

  (tr [{:person/gender :gender/male
        :person/full-name "Brian May"}

       {:person/gender :gender/male
        :person/full-name "Roger Taylor"}

       {:band/name "Queen"
        :band/country :country/england
        :band/members [[:person/full-name "Brian May"]
                       [:person/full-name "Roger Taylor"]]}])

  (let [e (resolve* [:band/name "Queen"])
        e1 (resolve* [:person/full-name "Brian May"])
        e2 (resolve* [:person/full-name "Roger Taylor"])

        p (api/pull *scope* '[* {:band/members [*]}] e)]

    (is (= (update p :band/members (partial sort-by :db/id))
           {:db/id e
            :band/name "Queen"
            :band/country #:db{:id 100000}
            :band/members
            [{:db/id e1
              :person/gender #:db{:id 100002}
              :person/full-name "Brian May"}
             {:db/id e2
              :person/gender #:db{:id 100002}
              :person/full-name "Roger Taylor"}]}))))


(deftest test-lookup-is-missing

  (with-thrown? #"Lookup failed"
    (tr [{:person/gender [:db/ident :gender/dunno]
          :person/full-name "Roger Taylor"}]))

  (with-thrown? #"Lookup failed"
    (tr [{:band/name "Queen"
          :band/country :country/russia}]))

  (with-thrown? #"Lookup failed"
    (tr [{:band/name "Queen"
          :band/country :country/england
          :band/members [[:person/full-name "Somebody Someone "]]}])))


(deftest test-insert-multiple-attr

  (tr [{:band/name "Queen"
        :band/country :country/england
        :band/genres ["rock1" "rock2" "rock2" "Rock1" " rock1 "]}])

  (let [e (resolve* [:band/name "Queen"])
        p (api/pull *scope* '[:band/name :band/genres] e)]

    (is (= p {:db/id e
              :band/name "Queen"
              :band/genres #{"rock1" "rock2" "Rock1" " rock1 "}}))))


(deftest test-upsert-eid-not-found

  (with-thrown? #"not found"
    (tr [{:db/id 99999999
          :band/name "Queen"}])))


(deftest test-upsert-simple

  (tr [{:band/name "Queen"}])

  (let [e (resolve* [:band/name "Queen"])
        p (api/pull *scope* '[*] e)]

    (is (= p {:db/id e
              :band/name "Queen"}))

    (tr [{:db/id e
          :band/name "Queen2"
          :band/country :country/england}])

    (let [p (api/pull *scope* '[*] e)]

      (is (= p {:db/id e
                :band/name "Queen2"
                :band/country #:db{:id 100000}})))))


(deftest test-upsert-multiple-attrl

  (tr [{:db/ident :rtaylor
        :person/full-name "Roger Taylor"}

       {:db/ident :bmay
        :person/full-name "Brian May"}

       {:db/ident :queen
        :band/name "Queen"
        :band/members [:rtaylor :bmay]}])

  (let [e (resolve* [:band/name "Queen"])]

    (tr [{:db/ident :jdeacon
          :person/full-name "John Deacon"}

         {:db/ident :queen
          :band/members [:jdeacon]}])

    (let [p (api/pull *scope* '[*] e)]
      (is (-> p :band/members set count (= 3))))))


(deftest test-upsert-by-unique-ident

  (tr [{:band/name "Queen"}])

  (tr [{:band/name "Queen"
        :band/country :country/england}])

  (let [p (pull* :band/name)]

    (is (= p [["band/country" "100000"]
              ["band/name" "Queen"]]))))


(deftest test-upsert-by-unique-ident-conflict

  (tr [{:band/name "Queen"}])

  (with-thrown? #"Uniqueness conflict"
    (tr [{:db/id 777777
          :band/name "Queen"
          :band/country :country/england}])))


(deftest test-upsert-by-unique-ident-merge

  (tr [{:band/name "Queen"}

       {:band/name "Queen"
        :band/country :country/england}

       {:band/name "Queen"
        :band/genres ["rock"]}])

  (let [e (resolve* [:band/name "Queen"])
        p (api/pull *scope* '[*] e)]

    (is (= p {:db/id e
              :band/name "Queen"
              :band/country #:db{:id 100000}
              :band/genres #{"rock"}}))))


(deftest test-ref-temp-id-not-resolved

  (with-thrown? #"Temp id .+ cannot be resolved"
    (tr [{:release/band "missing"}])))


(deftest test-eid-lookup-ident

  (tr [{:db/id "queen_id"
        :db/ident :queen
        :band/name "Queen"}])

  (tr [{:db/id [:band/name "Queen"]
        :band/country :country/england}])

  (tr [{:db/id :queen
        :band/genres #{"rock"}}])

  (let [p (pull* :band/name)]

    (is (= p '(["band/country" "100000"]
               ["band/genres" "rock"]
               ["band/name" "Queen"]
               ["db/ident" "queen"])))))


(deftest test-eid-lookup-ident-in-single-tr

  (tr [{:db/id "queen_id"
        :db/ident :queen
        :band/name "Queen"}

       {:db/id [:band/name "Queen"]
        :band/country :country/england}

       {:db/id :queen
        :band/genres #{"rock"}}

       {:db/id "queen_id"
        :band/genres #{"rock2"}}])

  (let [p (pull* :band/name)]

    (is (= p '(["band/country" "100000"]
               ["band/genres" "rock"]
               ["band/genres" "rock2"]
               ["band/name" "Queen"]
               ["db/ident" "queen"])))))


(deftest test-insert-ident-with-ref

  (tr [{:db/id "abba"
        :db/ident :abba
        :band/name "ABBA"}

       {:profile/band "abba"
        :profile/code "A1"}])

  (tr [{:profile/band [:band/name "ABBA"]
        :profile/code "A2"}])

  (let [e (resolve* [:db/ident :abba])
        p (pull* :profile/band)]
    (is (= p [["profile/band" (str e)]
              ["profile/code" "A2"]])))

  (tr [{:profile/band :abba
        :profile/code "A3"}])

  (let [e (resolve* [:db/ident :abba])
        p (pull* :profile/band)]
    (is (= p [["profile/band" (str e)]
              ["profile/code" "A3"]])))

  #_
  (tr [{:db/id "tempid"
        :profile/band :abba
        :profile/code "A3"}

       {:release/band "tempid"}

       ]

      )

  #_
  (tr [{:db/id 999999
        :profile/band :abba
        :profile/code "A3"}])



  )


;; test insert identity

;; test insert unique/value
;; test insert unique + ref + ident
