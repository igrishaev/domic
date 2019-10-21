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


(use-fixtures :once fix/fix-with-scope)
(use-fixtures :each fix/fix-raw-insert)


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


;; test upsert identity
;; test insert identity

;; test insert unique/value
;; test insert unique + ref + ident
