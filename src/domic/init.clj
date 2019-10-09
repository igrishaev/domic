(ns domic.init
  (:refer-clojure :exclude [< >])
  (:require
   [domic.util :as u]
   [domic.engine :as en]
   [domic.attr-manager :as am]
   [domic.util :refer [kw->str]]

   [clojure.java.jdbc :as jdbc]
   [honeysql.core :as sql]))

;; todo
;; fix transaction macros

(defn init-seq
  [{:as scope :keys [table-seq
                     en]}]
  (let [sql (sql/raw ["CREATE SEQUENCE IF NOT EXISTS " table-seq])]
    (en/execute en (sql/format sql))))


(def CREATE-IDX "CREATE INDEX IF NOT EXISTS idx_")
(def ON " ON ")
(def < " (")
(def > ")")
(def SPACE " ")
(def Q "'")

(defn init-indexes
  [{:as scope :keys [en am
                     table]}]

  ;; E
  (let [sql (sql/raw [CREATE-IDX table :_E ON table < :e >])]
    (en/execute en (sql/format sql)))

  ;; T
  (let [sql (sql/raw [CREATE-IDX table :_T ON table < :t >])]
    (en/execute en (sql/format sql)))

  ;; EA
  (let [sql (sql/raw [CREATE-IDX table :_EA ON table < "e,a" >])]
    (en/execute en (sql/format sql)))

  ;; Attributes: indexed, refs, unique
  (doseq [attr @am]

    ;; AV
    (when (or (am/index?  am attr)
              (am/ref?    am attr)
              (am/unique? am attr))

      (let [db-type (am/db-type am attr)]

        (let [sql (sql/raw [CREATE-IDX
                            table :_EA_ (namespace attr) '_ (name attr)
                            ON table
                            <
                            (if (= db-type :text)
                              :v
                              (sql/call :cast :v db-type))
                            >
                            SPACE
                            {:where [:= :a (sql/raw [Q (u/kw->str attr) Q])]}])]
          (en/execute en (sql/format sql)))))))


(def ddl-table
  [[:id :serial  "primary key"]
   [:e  :integer "not null"]
   [:a  :text    "not null"]
   [:v  :text    "not null"]
   [:t  :integer "not null"]])


(def ddl-table-log
  (conj ddl-table [:op :boolean "not null"]))


(defn init-tables
  [{:as scope :keys [en
                     table
                     table-log]}]

  (let [opt {:conditional? true}
        queries
        [(jdbc/create-table-ddl table ddl-table opt)
         (jdbc/create-table-ddl table-log ddl-table-log opt)]]

    (doseq [query queries]
      (en/execute en query))))


(defn init
  [{:as scope :keys [en]}]
  (en/with-tx [en en]
    (let [scope (assoc scope :en en)]
      (init-tables scope)
      (init-indexes scope)
      (init-seq scope))))
