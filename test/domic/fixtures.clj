(ns domic.fixtures
  (:require
   [domic.attributes :as at]
   [domic.api :as api]
   [domic.init :as init]

   [clojure.java.jdbc :as jdbc]))


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
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

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

   {:db/ident       :person/roles
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/many}

   ;; band
   {:db/ident       :band/code
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one}

   {:db/ident       :band/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   {:db/ident       :band/rating
    :db/valueType   :db.type/float
    :db/cardinality :db.cardinality/one}

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
    :person/roles ["guitar" "vocal"]
    :person/gender :gender/male}

   {:db/id "Roger Taylor"
    :person/full-name "Roger Taylor"
    :person/date-born #inst "1949-07-26"
    :person/roles ["drums" "vocal"]
    :person/gender :gender/male}

   {:db/id "Freddie Mercury"
    :person/full-name "Freddie Mercury"
    :person/date-born #inst "1946-09-05"
    :person/date-died #inst "1991-11-24"
    :person/roles ["vocal"]
    :person/gender :gender/male}

   {:db/id "John Deacon"
    :person/full-name "John Deacon"
    :person/date-born #inst "1951-08-19"
    :person/roles ["bass"]
    :person/gender :gender/male}

   {:db/ident :queen
    :band/name "Queen"
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
    :band/code :band-abba
    :band/rating 4.75
    :band/country :country/sweden
    :band/website "https://abbasite.com/"
    :band/date-from #inst "1972"
    :band/genres ["pop" "disco"]
    :band/members ["Benny Andersson"
                   "Anni-Frid Lyngstad"
                   "Agnetha Fältskog"
                   "Björn Ulvaeus"]}])


(def db-spec
  {:dbtype "postgresql"
   :dbname "test"
   :host "127.0.0.1"
   :user "ivan"
   :password "ivan"
   :assumeMinServerVersion "10"})


(def ^:dynamic *scope* nil)


(defn fix-with-scope [t]
  (let [opt {:prefix "_tests19_"
             :debug? true}]

    (binding [*scope* (api/->scope db-spec opt)]
      (t))))


(defn fix-test-db [t]

  (api/init *scope*)
  (api/transact *scope* attrs)
  (api/sync-attrs *scope*)
  (api/transact *scope* data)

  (t)

  (let [{:keys [en
                table
                table-log
                table-seq]} *scope*

        queries
        [(format "drop table %s" (name table))
         (format "drop table %s" (name table-log))
         (format "drop sequence %s" (name table-seq))]]

    (doseq [query queries]
      (en/execute en query))))


(def enumerate (partial map-indexed vector))


(defn attrs->rows
  [attr-maps]

  (let [t -1
        rows* (transient [])]
    (doseq [[index attr-map] (enumerate attr-maps)]
      (let [e (+ index 100000)]
        (doseq [[a v] attr-map]
          (conj! rows* {:e e :a a :v v :t t}))))
    (-> rows* persistent! not-empty)))


(defn fix-raw-insert [t]

  (init/init-tables *scope*)
  (init/init-seq *scope*)

  (let [{:keys [en table]} *scope*

        rows (concat (attrs->rows at/defaults)
                     (attrs->rows attrs))]

    (jdbc/insert-multi! db-spec table rows)

    (api/sync-attrs *scope*)

    (t)

    (jdbc/execute! db-spec (format "truncate %s" (name table)))))
