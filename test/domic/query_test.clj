(ns domic.query-test
  (:require
   [clojure.test
    :refer [is deftest testing use-fixtures
            run-tests]]

   [domic.fixtures :refer [fix-test-db *scope*]]
   [domic.api :as api]))


;; todo
;; global test transaction
;; deal with prefix name
;; reveal only for v attribute


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


(deftest test-lookup-in-pattern-error

  (let [query '[:find [?website ...]
                :where
                [?band :band/website [:band/name "Queen"]]]]

    (is (thrown-with-msg?
         Exception #"Cannot parse query"

         (api/q *scope* query)))))


(deftest test-bind-wrong-arity

  (let [query '[:find ?website
                :in ?name ?country
                :where
                [?band :band/name ?name]
                [?band :band/country ?country]
                [?band :band/website ?website]]]

    (is (thrown-with-msg?
         Exception #"IN arity mismatch"

         (api/q *scope* query)))))


(deftest test-lookup-as-param

  (let [query '[:find ?website .
                :in ?band
                :where
                [?band :band/website ?website]]

        band [:band/name "Queen"]

        result (api/q *scope* query band)]

    (is (= result "http://queenonline.com/"))))


(deftest test-bind-scalar

  (let [query '[:find ?website
                :in ?band ?country ?genre
                :where
                [?band :band/genres ?genre]
                [?band :band/country ?country]
                [?band :band/website ?website]]

        result (api/q *scope* query
                      [:band/name "Queen"]
                      :country/england
                      "rock")]

    (is (= result '(["http://queenonline.com/"])))))


(deftest test-bind-tuple

  (let [query '[:find ?website
                :in [_ ?band _ ?country _ ?genre _]
                :where
                [?band :band/genres ?genre]
                [?band :band/country ?country]
                [?band :band/website ?website]]

        ;; add some trash
        tuple '(1 [:band/name "Queen"]
                  AAA :country/england
                  false "rock" {:foo bar})

        result (api/q *scope* query tuple)]

    (is (= result '(["http://queenonline.com/"])))))


(deftest test-bind-tuple-arity

  (let [query '[:find ?website
                :in [_ ?band _ ?country _ ?genre _]
                :where
                [?band :band/genres ?genre]
                [?band :band/country ?country]
                [?band :band/website ?website]]

        tuple '(1 [:band/name "Queen"]
                  AAA :country/england
                  false "rock" {:foo bar} EXTRA)]

    (is (thrown-with-msg?
         Exception #"Tuple arity mismatch"

         (api/q *scope* query tuple)))))


(deftest test-bind-collection

  (let [query '[:find ?name
                :in [?genre ...] [?members ...]
                :where
                [?band :band/genres ?genre]
                [?band :band/members ?members]
                [?band :band/name ?name]]

        genres ["rock"]
        members [[:person/full-name "Brian May"]
                 [:person/full-name "John Deacon"]]

        result (api/q *scope* query genres members)]

    (is (= result '(["Queen"])))))


(deftest test-bind-relation

  (let [query '[:find ?name
                :in [[?country ?member _]]
                :where
                [?band :band/country ?country]
                [?band :band/members ?member]
                [?band :band/name ?name]]

        rel '[[:country/england [:person/full-name "Brian May"]       dunno]
              [:country/sweden  [:person/full-name "Benny Andersson"] nil]
              [100500           99998888                              AAA]]

        result (api/q *scope* query rel)]

    (is (= (sort result) '(["ABBA"] ["Queen"])))))


(deftest test-query-by-missing-attr

  (let [query '[:find ?person-name ?band-name
                :where
                [?p :person/date-died]
                [?p :person/full-name ?person-name]
                [?b :band/members ?p]
                [?b :band/name ?band-name]]

        result (api/q *scope* query)]

    (is (= (sort result) '(["Freddie Mercury" "Queen"])))))


(deftest test-operators-gt

  (let [query '[:find [?person-name ...]
                :in ?born-from
                :where
                [?p :person/full-name ?person-name]
                [?p :person/date-born ?date-born]
                [(> ?date-born ?born-from)]]

        born-from #inst "1950"

        result (api/q *scope* query born-from)]

    (is (= (sort result) '("Agnetha Fältskog" "John Deacon")))))


(deftest test-operators-eq

  (let [query '[:find ?name
                :where
                [?p :person/nick-name ?nick]
                [(= ?nick "Anna")]
                [?p :person/full-name ?name]]

        result (api/q *scope* query)]

    (is (= (sort result) '(["Agnetha Fältskog"])))))


(deftest test-bool-logic-not

  (let [query '[:find ?name
                :where
                [?b :band/name "Queen"]
                [?b :band/members ?p]
                [?p :person/full-name ?name]
                (not [(= ?name "Freddie Mercury")])]

        result (api/q *scope* query)]

    (is (= (sort result) '(["Brian May"]
                           ["John Deacon"]
                           ["Roger Taylor"])))))


(deftest test-bool-logic-or

  (let [query '[:find ?name
                :where
                [?b :band/name "Queen"]
                [?b :band/members ?p]
                [?p :person/full-name ?name]
                (or [(= ?name "Freddie Mercury")]
                    [(= ?name "Roger Taylor")])]

        result (api/q *scope* query)]

    (is (= (sort result) '(["Freddie Mercury"]
                           ["Roger Taylor"])))))


(deftest test-bool-logic-or2

  (let [query '[:find ?name
                :where
                [?b :band/name "Queen"]
                [?b :band/members ?p]
                [?p :person/full-name ?name]
                (or [(= ?name "Freddie Mercury")]
                    (not [(= ?name "Roger Taylor")]))]

        result (api/q *scope* query)]

    (is (= (sort result) '(["Brian May"]
                           ["Freddie Mercury"]
                           ["John Deacon"])))))


(deftest test-dataset-ok

  (let [query '[:find ?x
                :in $data
                :where
                [$data  _ _ ?b]
                [$data ?b _ ?c]
                [$data ?c _ ?x]]

        dataset [[1 2 3]
                 [3 4 5]
                 [5 6 0]]

        result (api/q *scope* query dataset)]

    (is (= (sort result) '([0])))))


(deftest test-dataset-multiple-join

  (let [query '[:find ?id ?name ?country
                :in $ds1 $ds2
                :where
                [$ds1 ?id ?name]
                [$ds2 ?id ?country]]

        ds1 [[1 "Queen"]
             [2 "Abba"]
             [3 "Korn"]
             [4 "Pink FLoyd"]]

        ds2 [[1 "GB"]
             [2 "Sweden"]
             [3 "USA"]
             [5 "Russia"]]

        result (api/q *scope* query ds1 ds2)]

    (is (= (sort result) '([1 "Queen" "GB"]
                           [2 "Abba" "Sweden"]
                           [3 "Korn" "USA"])))))


(deftest test-default-source-is-set

  (let [query '[:find ?x
                :in $
                :where
                [$data  _ _ ?b]]

        dataset [[1 2 3]
                 [3 4 5]
                 [5 6 0]]]

    (is (thrown-with-msg?
         Exception #"has already been added"

         (api/q *scope* query dataset)))))


(deftest test-dataset-table-mix

  (let [query '[:find ?band-name ?album-name ?website
                :in $albums
                :where
                [$albums ?band-name ?album-name]
                [?band :band/website ?website]
                [?band :band/name ?band-name]]

        albums [["Queen" "Innuendo"]
                ["Queen" "The Miracle"]
                ["ABBA" "Arrival"]
                ["ABBA" "Waterloo"]
                ["Korn" "Untouchables"]]

        result (api/q *scope* query albums)]

    (is (= (sort result)
           '(["ABBA" "Arrival" "https://abbasite.com/"]
             ["ABBA" "Waterloo" "https://abbasite.com/"]
             ["Queen" "Innuendo" "http://queenonline.com/"]
             ["Queen" "The Miracle" "http://queenonline.com/"])))))



(deftest test-dataset-table-mix2

  (let [query '[:find ?band-name ?album-name ?duration ?person-name ?role
                :in $albums $members
                :where
                [$albums ?band-name ?album-name ?duration]
                [?band :band/website ?website]
                [?band :band/name ?band-name]
                [$members ?band-name ?person-name ?role]
                [?band :band/members ?person]
                [?persion :person/full-name ?person-name]]

        albums [["Queen" "Innuendo"     "53:48"]
                ["Queen" "The Miracle"  "41:22"]
                ["ABBA"  "Arrival"      "37:31"]
                ["ABBA"  "Waterloo"     "38:10"]
                ["Korn"  "Untouchables" "65:00"]]

        members [["ABBA"  "Benny Andersson"  "vocal"]
                 ["ABBA"  "Agnetha Fältskog" "vocal"]
                 ["Queen" "John Deacon"      "bass"]
                 ["Queen" "Roger Taylor"     "drums"]
                 ["Korn"  "Jonathan Davis"   "vocal"]]

        result (api/q *scope* query albums members)]

    (is (= (sort result)
           '(["ABBA"  "Arrival"     "37:31" "Agnetha Fältskog" "vocal"]
             ["ABBA"  "Arrival"     "37:31" "Benny Andersson"  "vocal"]
             ["ABBA"  "Waterloo"    "38:10" "Agnetha Fältskog" "vocal"]
             ["ABBA"  "Waterloo"    "38:10" "Benny Andersson"  "vocal"]
             ["Queen" "Innuendo"    "53:48" "John Deacon"      "bass"]
             ["Queen" "Innuendo"    "53:48" "Roger Taylor"     "drums"]
             ["Queen" "The Miracle" "41:22" "John Deacon"      "bass"]
             ["Queen" "The Miracle" "41:22" "Roger Taylor"     "drums"])))))


(deftest test-basic-math-operators

  (let [query '[:find ?a ?b ?c ?d
                :where
                [(+  1  2) ?a]  ;; 3
                [(- ?a -1) ?b]  ;; 4
                [(* ?b  3) ?c]  ;; 12
                [(/ ?c  4) ?d]  ;; 3
                ]

        result (api/q *scope* query)]

    (is (= result '([3 4 12 3])))))


(deftest test-extended-math-operators

  (let [query '[:find ?a ?b ?c ?d ?e
                :where
                [(mod  5 4) ?a]    ;; 1
                [(exp  2 3) ?b]    ;; 8
                [(fact 5)   ?c]    ;; 120
                [(sqrt 25)  ?d]    ;; 5
                [(abs -5)   ?e]    ;; 5
                ]

        result (api/q *scope* query)]

    (is (= result '([1 8.0 120M 5.0 5])))))


;; check for aggregate
;; check rules
;; check builtin functions
;; check find patterns
;; check pull
;; check return maps (strings, symbols)
;; check with
;; check map form
;; check with no an attr in a query
