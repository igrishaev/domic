(ns domic.init
  (:require
   [domic.engine :as en]
   [domic.attr-manager :as am]
   [domic.util :refer [kw->str]]

   [clojure.java.jdbc :as jdbc]))

;; todo
;; fix transaction macros
;; get rid of bare jdbc

(defn init-seq
  [{:as scope :keys [table-seq
                     en]}]
  (let [sql (format "CREATE SEQUENCE IF NOT EXISTS %s"
                    (name table-seq))]
    (jdbc/execute! @en sql)))


(defn sql-index-av
  [table attr db-type]
  (let [table-name (kw->str table)
        type-name (kw->str db-type)
        attr-name
        (let [ans (namespace attr)
              aname (name attr)]
          (format "%s_%s" ans aname))
        index-name (format "idx_%s_AV_%s" table-name attr-name)]
    (format "CREATE INDEX IF NOT EXISTS %s ON %s (CAST(v AS %s)) WHERE a = '%s'"
            index-name table-name type-name attr-name)))


(defn init-indexes
  [{:as scope :keys [en am
                     table]}]

  ;; E
  (let [index-name (format "idx_%s_E" (name table))
        index-sql (format "CREATE INDEX IF NOT EXISTS %s ON %s (e)"
                          index-name (name table))]
    (jdbc/execute! @en index-sql))

  ;; T
  (let [index-name (format "idx_%s_T" (name table))
        index-sql (format "CREATE INDEX IF NOT EXISTS %s ON %s (t)"
                          index-name (name table))]
    (jdbc/execute! @en index-sql))

  ;; Attributes: indexed, refs, unique
  (doseq [attr @am]

    ;; EV
    (let [index-name (format "idx_%s_EA" (name table))
          index-sql (format "CREATE INDEX IF NOT EXISTS %s ON %s (e,a)"
                            index-name (name table))]
      (jdbc/execute! @en index-sql))

    ;; AV
    (when (or (am/index?  am attr)
              (am/ref?    am attr)
              (am/unique? am attr))

      (let [db-type (am/db-type am attr)
            sql-index (sql-index-av table attr db-type)]
        (jdbc/execute! @en sql-index)))))


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
        tables
        [(jdbc/create-table-ddl table ddl-table opt)
         (jdbc/create-table-ddl table-log ddl-table-log opt)]]

    (doseq [table tables]
      (jdbc/execute! @en table))))


(defn init
  [{:as scope :keys [en]}]
  (en/with-tx [en en]
    (let [scope (assoc scope :en en)]
      (init-tables scope)
      (init-indexes scope)
      (init-seq scope))))
