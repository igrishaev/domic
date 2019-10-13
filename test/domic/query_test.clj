(ns domic.query-test
  (:require
   [clojure.test :refer [is deftest testing use-fixtures]]

   [domic.api :as api]
   [domic.engine :as en]))

;; todo
;; global test transaction
;; deal with prefix name

(def attrs

  [;; countries
   {:db/ident       :country/england}
   {:db/ident       :country/sweden}

   ;; gender
   {:db/ident       :gender/male}
   {:db/ident       :gender/female}

   ;; person
   {:db/ident       :person/full-name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :person/nick-name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :person/date-born
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}

   {:db/ident       :person/date-died
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}

   {:db/ident       :person/gender
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}

   ;; band
   {:db/ident       :band/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   {:db/ident       :band/country
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident       :band/website
    :db/valueType   :db.type/uri
    :db/cardinality :db.cardinality/one}

   {:db/ident       :band/members
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many}

   {:db/ident       :band/date-from
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}

   {:db/ident       :band/date-to
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}

   {:db/ident       :band/genres
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/many}

   ;; release
   {:db/ident       :release/title
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :release/date
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}

   {:db/ident       :release/band
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}

   {:db/ident       :release/label
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :release/rating
    :db/valueType   :db.type/float
    :db/cardinality :db.cardinality/one}])


(def data

  [;; Queen

   {:db/id "Brian May"
    :person/full-name "Brian May"
    :person/date-born #inst "1947-07-19"
    :person/gender :gender/male}

   {:db/id "Roger Taylor"
    :person/full-name "Roger Taylor"
    :person/date-born #inst "1949-07-26"
    :person/gender :gender/male}

   {:db/id "Freddie Mercury"
    :person/full-name "Freddie Mercury"
    :person/date-born #inst "1946-09-05"
    :person/date-died #inst "1991-11-24"
    :person/gender :gender/male}

   {:db/id "John Deacon"
    :person/full-name "John Deacon"
    :person/date-born #inst "1951-08-19"
    :person/gender :gender/male}

   {:band/name "Queen"
    :band/country :country/england
    :band/website "http://queenonline.com/"
    :band/date-from #inst "1970"
    :band/genres ["rock" "rock'n'roll"]
    :band/members ["Brian May"
                   "Roger Taylor"
                   "Freddie Mercury"
                   "John Deacon"]}

   ;; Abba

   {:db/id "Benny Andersson"
    :person/full-name "Benny Andersson"
    :person/date-born #inst "1946-12-16"
    :person/gender :gender/male}

   {:db/id "Anni-Frid Lyngstad"
    :person/full-name "Anni-Frid Lyngstad"
    :person/date-born #inst "1945-11-15"
    :person/gender :gender/female}

   {:db/id "Agnetha Fältskog"
    :person/full-name "Agnetha Fältskog"
    :person/nick-name "Anna"
    :person/date-born #inst "1950-04-05"
    :person/gender :gender/female}

   {:db/id "Björn Ulvaeus"
    :person/full-name "Björn Ulvaeus"
    :person/date-born #inst "1945-04-25"
    :person/gender :gender/male}

   {:band/name "ABBA"
    :band/country :country/sweden
    :band/website "https://abbasite.com/"
    :band/date-from #inst "1972"
    :band/genres ["pop" "disco"]
    :band/members ["Benny Andersson"
                   "Anni-Frid Lyngstad"
                   "Agnetha Fältskog"
                   "Björn Ulvaeus"]}])


(def ^:dynamic *scope* nil)

(def db-spec
  {:dbtype "postgresql"
   :dbname "test"
   :host "127.0.0.1"
   :user "ivan"
   :password "ivan"
   :assumeMinServerVersion "10"})


(defn fix-test-db [t]

  (let [opt {:prefix "_tests14_"
             :debug? true}]

    (binding [*scope* (api/->scope db-spec opt)]

      ;; (api/sync-attrs *scope*)

      (api/init *scope*)
      (api/transact *scope* attrs)
      (api/sync-attrs *scope*)
      (api/transact *scope* data)

      (t)

      (let [{:keys [en
                    table
                    table-log
                    table-seq]} *scope*]

        (en/execute en (format "drop table %s" (name table)))
        (en/execute en (format "drop table %s" (name table-log)))
        (en/execute en (format "drop sequence %s"
                               (name table-seq)))))))


(use-fixtures :once fix-test-db)


(deftest test-primitive

  (let [query '[:find [?name ...]
                :where
                [?a :band/name ?name]]
        result (api/q *scope* query)]

    (is (= (sort result) '("ABBA" "Queen")))))


(deftest test-simple-join

  (let [query '[:find ?band-name ?person-name
                :where
                [?person :person/full-name ?person-name]
                [?band :band/members ?person]
                [?band :band/name ?band-name]]

        result (api/q *scope* query)]

    (is (=
         (sort result)
         '
         (["ABBA" "Agnetha Fältskog"]
          ["ABBA" "Anni-Frid Lyngstad"]
          ["ABBA" "Benny Andersson"]
          ["ABBA" "Björn Ulvaeus"]
          ["Queen" "Brian May"]
          ["Queen" "Freddie Mercury"]
          ["Queen" "John Deacon"]
          ["Queen" "Roger Taylor"])))))


(deftest test-ident-ok

  (let [query '[:find [?band-name ...]
                :where
                [?band :band/name ?band-name]
                [?band :band/members ?person]
                [?person :person/gender :gender/female]]

        result (api/q *scope* query)]

    (is (= (sort result) '("ABBA")))))


(deftest test-ident-missing

  (let [query '[:find [?person ...]
                :where
                [?person :person/gender :gender/dunno]]]

    (is (thrown-with-msg?
         Exception #"Lookup failed"

         (api/q *scope* query)))))


(deftest test-find-by-multiple-tag

  (let [query '[:find [?band-name ...]
                :in [?genre ...]
                :where
                [?band :band/genres ?genre]
                [?band :band/name ?band-name]]]

    (doseq [[genres result]
            [[["rock" "disco"]       ["ABBA" "Queen"]]
             [["dunno" "disco"]      ["ABBA"]]
             [["rock" "dunno"]       ["Queen"]]
             [["Rock" "disco"]       ["ABBA"]]
             [["rock" "Disco"]       ["Queen"]]
             [[" rock " "\ndisco\t"] []]
             [["rock" "disco" "foo"] ["ABBA" "Queen"]]]]

      (let [result* (api/q *scope* query genres)]
        (is (= (sort result) (sort result*)))))))


(deftest test-find-result-types

  (let [query '[:find
                ?name
                ?country
                ?website
                ?members
                ?date-from
                ?genre
                :in ?name
                :where
                [?band :band/name      ?name]
                [?band :band/country   ?country]
                [?band :band/website   ?website]
                [?band :band/members   ?members]
                [?band :band/date-from ?date-from]
                [?band :band/genres    ?genre]]

        result (api/q *scope* query "Queen")

        [?name
         ?country
         ?website
         ?members
         ?date-from
         ?genre] (-> result sort first )]

    (is (= ?name "Queen"))
    (is (int? ?country))
    (is (= ?website "http://queenonline.com/"))
    (is (int? ?members))
    (is (zero? (compare ?date-from #inst "1970-01-01T00:00:00")))
    (is (= ?genre "rock"))))


(deftest test-missing-attribute

  (let [query '[:find [?name ...]
                :where
                [?a :band/dunno ?name]]]

    (is (thrown-with-msg?
         Exception #"Unknown attrubute"

         (api/q *scope* query)))))

;; check for aggregate
