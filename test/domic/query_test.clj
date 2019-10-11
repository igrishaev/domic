(ns domic.query-test
  (:require
   [clojure.test :refer [is deftest testing]]
   [domic.api :as api]))


(def attrs

  [;; artist

   {:db/ident       :artist/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :artist/tag
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/many}

   ;; release
   {:db/ident       :release/artist
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/isComponent true}

   {:db/ident       :release/year
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident       :release/tag
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/many}])


(def data

  [{:db/id "queen"
    :db/ident :queen
    :artist/name "Queen"
    :artist/tag ["Rock" "Rock'n'Roll"]}

   {:db/id "abba"
    :db/ident :abba
    :artist/name "Abba"
    :artist/tag ["Pop" "Disco"]}

   {:release/artist "queen"
    :release/year 1985
    :release/tag ["Tape" "Vynil"]}

   {:release/artist "abba"
    :release/year 1975
    :release/tag ["Vynil" "Radio"]}])


(def db-spec
  {:dbtype "postgresql"
   :dbname "test"
   :host "127.0.0.1"
   :user "ivan"
   :password "ivan"
   :assumeMinServerVersion "10"})


(deftest test-aaaaa

  (let [opt {:prefix "_tests_"
             :debug? true}

        scope (api/->scope db-spec opt)

        ;; _ (api/init scope)
        ;; _ (api/transact scope attrs)

        _ (api/sync-attrs scope)

        ;; _ (api/transact scope data)

        query '[:find [?name ...]
                :in ?test
                :where
                [?a :artist/name ?name]]

        result (api/q scope query "test")]

    (is (= result 1))))
