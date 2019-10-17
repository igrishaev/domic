(ns domic.query-test
  (:require
   [clojure.test
    :refer [is deftest testing use-fixtures
            run-tests]]

   [domic.fixtures :refer [fix-test-db *scope*]]
   [domic.api :as api])

  (:import
   (java.util UUID Date)
   java.net.URI))


;; todo
;; global test transaction
;; deal with prefix name
;; reveal only for v attribute


(use-fixtures :once fix-test-db)


(defmacro with-thrown?
  [re & body]
  `(~'is (~'thrown-with-msg?
          Exception ~re ~@body)))


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

    (with-thrown? #"Lookup failed"
      (api/q *scope* query))))


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

    (with-thrown? #"Unknown attrubute"
      (api/q *scope* query))))


(deftest test-lookup-in-pattern-error

  (let [query '[:find [?website ...]
                :where
                [?band :band/website [:band/name "Queen"]]]]

    (with-thrown? #"Cannot parse query"
      (api/q *scope* query))))


(deftest test-bind-wrong-arity

  (let [query '[:find ?website
                :in ?name ?country
                :where
                [?band :band/name ?name]
                [?band :band/country ?country]
                [?band :band/website ?website]]]

    (with-thrown? #"IN arity mismatch"
      (api/q *scope* query))))


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

    (with-thrown? #"Tuple arity mismatch"
      (api/q *scope* query tuple))))


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

    (with-thrown? #"has already been added"
      (api/q *scope* query dataset))))


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


(deftest test-function-concat

  (let [query '[:find ?concat
                :where
                [?band :band/name ?band-name]
                [?band :band/members ?person]
                [?person :person/full-name ?person-name]
                [(concat ?person-name " performs in " ?band-name) ?concat]]

        result (api/q *scope* query)]

    (is (= (->> result sort (take 2))
           '(["Agnetha Fältskog performs in ABBA"]
             ["Anni-Frid Lyngstad performs in ABBA"])))))


;; todo
;; check source var

(deftest test-function-get-some-default-nil

  (let [query '[:find ?person-name ?date-died
                :in ?default
                :where
                [?person :person/full-name ?person-name]
                [(get-else $ ?person :person/date-died ?default) ?date-died]]

        result (api/q *scope* query nil)

        ->millis (fn [value]
                   (when value
                     (.getTime ^java.util.Date value)))]

    (is (= (->> result sort
                (map #(update % 1 ->millis)))

           '(["Agnetha Fältskog"   nil]
             ["Anni-Frid Lyngstad" nil]
             ["Benny Andersson"    nil]
             ["Björn Ulvaeus"      nil]
             ["Brian May"          nil]
             ["Freddie Mercury"    690940800000]
             ["John Deacon"        nil]
             ["Roger Taylor"       nil])))))


(deftest test-function-get-some-default-wrong-type

  (let [query '[:find ?person-name ?date-died
                :in ?default
                :where
                [?person :person/full-name ?person-name]
                [(get-else $ ?person :person/date-died ?default) ?date-died]]]

    (with-thrown? #"ERROR: UNION types .+ cannot be matched"
      (api/q *scope* query "N/A"))))


;; todo
;; check src var

(deftest test-predicate-missing

  (let [query '[:find ?person-name
                :where
                [?person :person/full-name ?person-name]
                [(missing? $ ?person :person/date-died)]]

        result (api/q *scope* query)]

    (is (= (sort result)
           '(["Agnetha Fältskog"]
             ["Anni-Frid Lyngstad"]
             ["Benny Andersson"]
             ["Björn Ulvaeus"]
             ["Brian May"]
             ["John Deacon"]
             ["Roger Taylor"])))))


(deftest test-find-spec-rel

  (let [query '[:find ?a ?b ?c
                :in ?a ?b ?c]

        result (api/q *scope* query 1 2 3)]

    (is (= result [[1 2 3]]))))


(deftest test-find-spec-tuple

  (let [query '[:find [?a ?b ?c]
                :in ?a ?b ?c]

        result (api/q *scope* query 1 2 3)]

    (is (= result [1 2 3]))))


(deftest test-find-spec-coll

  (let [query '[:find [?a ...]
                :in ?a ?b ?c]

        result (api/q *scope* query 1 2 3)]

    (is (= result [1]))))


(deftest test-find-spec-scalar

  (let [query '[:find ?a .
                :in ?a ?b ?c]

        result (api/q *scope* query 1 2 3)]

    (is (= result 1))))


(deftest test-maps-keys-keywords

  (let [query '[:find ?band-name ?person-name
                :keys band person
                :where
                [?band :band/name ?band-name]
                [?band :band/members ?person]
                [?person :person/full-name ?person-name]]

        result (api/q *scope* query)]

    (is (= (sort-by (juxt :band :person) result)
           '({:band "ABBA", :person "Agnetha Fältskog"}
             {:band "ABBA", :person "Anni-Frid Lyngstad"}
             {:band "ABBA", :person "Benny Andersson"}
             {:band "ABBA", :person "Björn Ulvaeus"}
             {:band "Queen", :person "Brian May"}
             {:band "Queen", :person "Freddie Mercury"}
             {:band "Queen", :person "John Deacon"}
             {:band "Queen", :person "Roger Taylor"})))))


(deftest test-maps-keys-strings

  (let [query '[:find ?band-name ?person-name
                :strs prefix/band prefix/person
                :where
                [?band :band/name ?band-name]
                [?band :band/members ?person]
                [?person :person/full-name ?person-name]]

        result (api/q *scope* query)]

    (is (= (sort-by (juxt #(get % "prefix/band")
                          #(get % "prefix/person")) result)
           '({"prefix/band" "ABBA", "prefix/person" "Agnetha Fältskog"}
             {"prefix/band" "ABBA", "prefix/person" "Anni-Frid Lyngstad"}
             {"prefix/band" "ABBA", "prefix/person" "Benny Andersson"}
             {"prefix/band" "ABBA", "prefix/person" "Björn Ulvaeus"}
             {"prefix/band" "Queen", "prefix/person" "Brian May"}
             {"prefix/band" "Queen", "prefix/person" "Freddie Mercury"}
             {"prefix/band" "Queen", "prefix/person" "John Deacon"}
             {"prefix/band" "Queen", "prefix/person" "Roger Taylor"})))))

(deftest test-maps-keys-symbols

  (let [query '[:find ?band-name ?person-name
                :syms prefix/band prefix/person
                :where
                [?band :band/name ?band-name]
                [?band :band/members ?person]
                [?person :person/full-name ?person-name]]

        result (api/q *scope* query)]

    (is (= (sort-by (juxt #(get % 'prefix/band)
                          #(get % 'prefix/person)) result)

           '({prefix/band "ABBA",  prefix/person "Agnetha Fältskog"}
             {prefix/band "ABBA",  prefix/person "Anni-Frid Lyngstad"}
             {prefix/band "ABBA",  prefix/person "Benny Andersson"}
             {prefix/band "ABBA",  prefix/person "Björn Ulvaeus"}
             {prefix/band "Queen", prefix/person "Brian May"}
             {prefix/band "Queen", prefix/person "Freddie Mercury"}
             {prefix/band "Queen", prefix/person "John Deacon"}
             {prefix/band "Queen", prefix/person "Roger Taylor"})))))


(deftest test-maps-keys-with-coll

  (let [query '[:find ?band-name
                :keys band/name
                :where
                [_ :band/name ?band-name]]

        result (api/q *scope* query)]

    (is (= (->> result (sort-by :band/name))
           [#:band{:name "ABBA"} #:band{:name "Queen"}]))))


(deftest test-maps-keys-with-tuple

  (let [query '[:find [?band-name ?band-rating]
                :keys band/name band/rating
                :where
                [?band :band/name ?band-name]
                [?band :band/country :country/sweden]
                [?band :band/rating ?band-rating]]

        result (api/q *scope* query)]

    (is (= result {:band/name "ABBA" :band/rating 4.75}))))


(deftest test-maps-keys-with-scalar

  (let [query '[:find ?band-name .
                :keys band/name
                :where
                [?band :band/name ?band-name]
                [?band :band/country :country/sweden]]

        result (api/q *scope* query)]

    (is (= result {:band/name "ABBA"}))))


(deftest test-maps-keys-arity-mismatch

  (let [query1 '[:find ?band-name ?band-country
                 :keys band/name
                 :where
                 [?band :band/name ?band-name]
                 [?band :band/country ?band-country]]

        query2 '[:find [?band-name ?band-country]
                 :keys band/name
                 :where
                 [?band :band/name ?band-name]
                 [?band :band/country ?band-country]]

        query3 '[:find [?band-name ...]
                 :keys band/name band/test
                 :where
                 [_ :band/name ?band-name]]

        query4 '[:find ?band-name .
                 :keys band/name band/test
                 :where
                 [_ :band/name ?band-name]]]

    (doseq [q [query1 query2 query3 query4]]

      (with-thrown? #"Find/keys arity mismatch"
        (api/q *scope* q)))))


(deftest test-with-clause-simple

  (let [query '[:find ?gender-ident
                :with ?person
                :where
                [?person :person/gender ?gender]
                [?gender :db/ident ?gender-ident]]

        result (api/q *scope* query)]

    (is (= (count result) 8))
    (is (= (set result)
           #{["gender/male"] ["gender/female"]}))))


(deftest test-no-with-clause-items-distinct

  (let [query '[:find ?role
                :where
                [?band :band/name "Queen"]
                [?band :band/members ?person]
                [?person :person/roles ?role]]

        result (api/q *scope* query)]

    (is (= (sort result)
           '(["bass"] ["drums"] ["guitar"] ["vocal"])))))


(deftest test-with-clause-items-duplicate

  (let [query '[:find ?role
                :with ?person
                :where
                [?band :band/name "Queen"]
                [?band :band/members ?person]
                [?person :person/roles ?role]]

        result (api/q *scope* query)]

    (is (= (sort result)
           '(["bass"] ["drums"] ["guitar"]
             ["vocal"] ["vocal"] ["vocal"])))))


(deftest test-aggregate-role

  (let [query '[:find (count ?role) .
                :where
                [?band :band/name "Queen"]
                [?band :band/members ?person]
                [?person :person/roles ?role]]

        result (api/q *scope* query)]

    (is (= result 6))))


(deftest test-aggregate-role-by-person

  (let [query '[:find (count ?role) ?name
                :where
                [?band :band/name "Queen"]
                [?band :band/members ?person]
                [?person :person/roles ?role]
                [?person :person/full-name ?name]]

        result (api/q *scope* query)]

    (is (= (sort result)
           '([1 "Freddie Mercury"]
             [1 "John Deacon"]
             [2 "Brian May"]
             [2 "Roger Taylor"])))))


(deftest test-find-with-no-from-with-dataset

  (let [query '[:find [?heads ...]
                :in [[_ ?heads]]]

        data [["Cerberus" 3]
              ["Medusa" 1]
              ["Cyclops" 1]
              ["Chimera" 1]]

        result (api/q *scope* query data)]

    (is (= (sort result) '[1 3]))))


(deftest test-aggregate-heads

  ;; https://docs.datomic.com/on-prem/query.html#with

  (let [query '[:find (sum ?heads) .
                :in [[_ ?heads]]]

        data [["Cerberus" 3]
              ["Medusa" 1]
              ["Cyclops" 1]
              ["Chimera" 1]]

        result (api/q *scope* query data)]

    (is (= result 6M))))


(deftest test-aggregate-heads-by-creature

  (let [query '[:find ?monster (sum ?heads)
                :in [[?monster ?heads]]]

        data [["Cerberus" 3]
              ["Medusa" 1]
              ["Cyclops" 1]
              ["Chimera" 1]]

        result (api/q *scope* query data)]

    (is (= (sort result)
           '(["Cerberus" 3M]
             ["Chimera" 1M]
             ["Cyclops" 1M]
             ["Medusa" 1M])))))


(deftest test-aggregate-min-max-avg

  (let [query '[:find ?name (min ?mark) (max ?mark) (avg ?mark)
                :in [[?name ?mark]]]

        data [["Ivan" 3]
              ["Ivan" 5]
              ["Juan" 2]
              ["Juan" 4.5]
              ["Ioann" 1.99]]

        result (api/q *scope* query data)]

    (is (= (sort result)
           '(["Ioann" 1.99 1.99 1.99]
             ["Ivan" 3.0 5.0 4.0]
             ["Juan" 2.0 4.5 3.25])))))


(deftest test-aggregate-pg-string_agg

  (let [query '[:find (string_agg ?name ",")
                :in [[?name ?mark]]]

        data [["Ivan" 3]
              ["Ivan" 5]
              ["Juan" 2]
              ["Juan" 4.5]
              ["Ioann" 1.99]]

        result (api/q *scope* query data)]

    (is (= result '(["Ivan,Ivan,Juan,Juan,Ioann"])))))


(deftest test-join-tx-data

  (let [query '[:find ?band ?band ?inst
                :where
                [?band :band/name "Queen" ?tx]
                [?tx :db/txInstant ?inst]]

        result (api/q *scope* query)

        [[band tx inst]] result]

    (is (int? band))
    (is (int? tx))
    (is (inst? inst))))


(def rules
    '
    [[(queen? ?e)
      [?e :band/name "Queen"]]

     [(band-info [?e] ?name ?rating ?country ?website)
      [?e :band/name ?name]
      [?e :band/rating ?rating]
      [?e :band/country ?country-ref]
      [?country-ref :db/ident ?country]
      [?e :band/website ?website]]

     [(band-info-defaults [?e] ?name ?rating ?country ?website)
      [?e :band/name ?name]
      [(get-else $ ?e :band/rating 999) ?rating]
      [?e :band/country ?country-ref]
      [?country-ref :db/ident ?country]
      [?e :band/website ?website]]

     [(band-info-vars-mapping [?e] ?n ?r ?c ?w)
      [?e :band/name ?n]
      [(get-else $ ?e :band/rating 999) ?r]
      [?e :band/country ?c-ref]
      [?c-ref :db/ident ?c]
      [?e :band/website ?w]]

     [(get-band-names ?name)
      [_ :band/name ?name]]

     [(is-alive? [?p] ?pname)
      [(missing? $ ?p :person/date-died)]
      [?p :person/full-name ?pname]]

     ])


(deftest test-rule-simple

  (let [query '[:find ?band-name
                :in %
                :where
                [?band :band/name ?band-name]
                (queen? ?band)]

        result (api/q *scope* query rules)]

    (is (= result [["Queen"]]))))


(deftest test-rule-band-info

  (let [query '[:find ?band ?name ?rating ?country ?website
                :in %
                :where
                [?band :band/name "ABBA"]
                (band-info ?band ?name ?rating ?country ?website)]

        result (api/q *scope* query rules)
        [row] result]

    (is (= (count result) 1))

    (is (= (drop 1 row)
           ["ABBA" 4.75 "country/sweden" "https://abbasite.com/"]))))


(deftest test-rule-band-fails-for-missing-art

  (let [query '[:find ?band ?name ?rating ?country ?website
                :in %
                :where
                [?band :band/name "Queen"]
                (band-info ?band ?name ?rating ?country ?website)]

        result (api/q *scope* query rules)]

    (is (= (count result) 0))))


(deftest test-rule-band-defaults

  (let [query '[:find ?band ?name ?rating ?country ?website
                :in %
                :where
                [?band :band/name "Queen"]
                (band-info-defaults ?band ?name ?rating ?country ?website)]

        result (api/q *scope* query rules)
        [row] result]

    (is (= (count result) 1))
    (is (= (drop 1 row)
           '("Queen" 999.0 "country/england" "http://queenonline.com/")))))


(deftest test-rule-band-info-var-mapping

  (let [query '[:find ?band ?name ?rating ?country ?website
                :in %
                :where
                [?band :band/name "Queen"]
                (band-info-vars-mapping
                 ?band ?name ?rating ?country ?website)]

        result (api/q *scope* query rules)
        [row] result]

    (is (= (count result) 1))
    (is (= (drop 1 row)
           '("Queen" 999.0 "country/england" "http://queenonline.com/")))))


(deftest test-rule-required-vars-not-bound

  (let [query '[:find ?band ?name ?rating ?country ?website
                :in %
                :where
                (band-info ?band ?name ?rating ?country ?website)]]

    (with-thrown? #"Var \?band is unbound"
      (api/q *scope* query rules))))


(deftest test-rule-not-found

  (let [query '[:find ?band
                :in %
                :where
                (dunno ?band)]]

    (with-thrown? #"No such a rule: dunno"
      (api/q *scope* query rules))))


(deftest test-rule-broken-rules-passed

  (let [query '[:find ?band
                :in %
                :where
                (dunno ?band)]]

    (with-thrown? #"Wrong rules"
      (api/q *scope* query [:foo {:bar "AAA"}]))))


(deftest test-rule-non-required-var-get-bound-from-inside

  (let [query '[:find ?name
                :in %
                :where
                (get-band-names ?name)]

        result (api/q *scope* query rules)]

    (is (= (sort result)
           [["ABBA"] ["Queen"]]))))


(deftest test-rule-missing-attribute

  (let [query '[:find [?pname ...]
                :in %
                :where
                [?b :band/name "Queen"]
                [?b :band/members ?p]
                (is-alive? ?p ?pname)]

        result (api/q *scope* query rules)]

    (is (= (sort result)))))


(deftest test-attr-not-specified

  (let [query '[:find ?e ?a ?v
                :where
                [?e :band/name "Queen"]
                [?e ?a ?v]]

        result (api/q *scope* query)
        a-set (set (map second result))]

    (is (= (count result) 10))
    (is (= a-set #{"band/name" "band/genres"
                   "band/country" "band/members"
                   "band/date-from" "band/website"}))

    (is (every? string? (map peek result)))))


(deftest test-types-coercion

  (let [query '[:find ?kw ?sym ?str ?bool
                      ?long ?bigint ?float
                      ?inst ?uuid ?uri

                :in [[?kw ?sym ?str ?bool
                      ?long ?bigint ?float
                      ?inst ?uuid ?uri
                      ]]]

        data [[:keyword
               'symbol
               "string"
               true
               99
               999M
               9.99
               #inst "2033"
               #uuid "6eb0f888-6479-4631-b514-f1fc2c1077a6"
               (new URI "http://test.com/?foo=bar")]]

        result (api/q *scope* query data)

        [row] result

        [?kw ?sym ?str ?bool
         ?long ?bigint ?float
         ?inst ?uuid ?uri] row]


    (is (= (count result) 1))

    (is (= ?kw "keyword"))
    (is (= ?sym "symbol"))
    (is (= ?str "string"))
    (is (= ?bool true))
    (is (= ?long 99))
    (is (= ?bigint 999M))
    (is (= ?float 9.99))
    (is (zero? (compare ?inst #inst "2033")))))

;; check nested rules

;; check ident/lookups for e/ref only

;; check pull
;; check with not an attr in a query
;; check foreign table

;; get-else var from CTE
;; missing source var

;; check other types: keyword, symbol, UUID, URI, etc
;; check bytes
