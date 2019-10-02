(ns domic.pull
  (:require
   [clojure.string :as str]
   [clojure.spec.alpha :as s]

   [domic.runtime :refer [resolve-lookup!]]
   [domic.util :refer
    [kw->str drop-nils sym-generator]]
   [domic.error :refer [error!]]
   [domic.query-builder :as qb]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]
   [domic.sql-helpers :refer
    [->cast lookup? adder]]

   [honeysql.core :as sql]
   [datomic-spec.core :as ds])

  (:import java.sql.ResultSet))


(def conj-set (fnil conj #{}))


(defn- rs->datom
  [{:as scope :keys [am]}
   ^ResultSet rs]
  (let [attr (keyword (.getString rs 3))
        attr-type (am/get-db-type am attr)]
    {:e (.getLong rs 2)
     :a attr
     :v (am/rs->clj attr-type rs 4)
     :t (.getLong rs 5)}))


(defn rs->maps
  [{:as scope :keys [am]}]
  (fn [^ResultSet rs]
    (vals
     (loop [next? (.next rs)
            result {}]
       (if next?
         (let [{:as row :keys [e a v]} (rs->datom scope rs)]
           (recur (.next rs)
                  (-> (if (am/multiple? am a)
                        (update-in result [e a] conj-set v)
                        (assoc-in result [e a] v))
                      (assoc-in [e :db/id] e))))
         result)))))


(defn- rs->datoms
  [scope]
  (fn [^ResultSet rs]
    (let [result* (transient [])]
      (while (.next rs)
        (conj! result* (rs->datom scope rs)))
      (persistent! result*))))


(def WC '*)


(defn backref?
  [attr]
  (some-> attr name (str/starts-with? "_")))


(defn split-pattern
  [pattern]
  (let [pattern (set pattern)

        wc? (or (contains? pattern WC)
                (contains? pattern (str WC)))

        attrs* (transient #{})
        refs* (transient {})
        backrefs* (transient {})]

    (doseq [pattern pattern]

      (cond

        (keyword? pattern)
        (if (backref? pattern)
          (assoc! backrefs* pattern [WC])
          (conj! attrs* pattern))


        (map? pattern)
        (doseq [[attr pattern] pattern]
          (if (backref? attr)
            (assoc! backrefs* attr pattern)
            (assoc! refs* attr pattern)))

        (= pattern WC) nil
        (= pattern (str WC)) nil

        :else
        (error! "Wrong pattern: %s" pattern)))

    {:wc? wc?
     :attrs (persistent! attrs*)
     :refs (persistent! refs*)
     :backrefs (persistent! backrefs*)}))


(defn pull*
  [{:as scope :keys [en]}
   ids & [attrs]]

  (let [params* (transient {})
        params*add (adder params*)

        ids*   (mapv params*add ids)
        attrs* (mapv params*add attrs)

        sql (sql/build
             :select :*
             :from :datoms4
             :where [:and
                     [:in :e ids*]
                     (when attrs [:in :a attrs*])])

        params (persistent! params*)
        query (sql/format sql params)]

    (en/query-rs en query (rs->maps scope))))


(defn pull*-refs
  [{:as scope :keys [en]}
   ids-ref attrs-ref & [attrs]]

  (let [params*    (transient {})
        params*add (adder params*)

        ids-ref*   (mapv params*add ids-ref)
        attrs-ref* (mapv params*add attrs-ref)
        attrs*     (mapv params*add attrs)

        v-cast (sql/call :cast :v :integer)

        sub (sql/build
             :select :e
             :from :datoms4
             :where [:and
                     [:in v-cast ids-ref*]
                     [:in :a attrs-ref*]])

        sql (sql/build
             :select :* :from :datoms4
             :where [:and
                     [:in :e sub]
                     (when (seq attrs)
                       [:in :a attrs*])])

        params (persistent! params*)
        query (sql/format sql params)]

    (en/query-rs en query (rs->maps scope))))


(defn pull-join
  [{:as scope :keys [am]}
   p1 p2 attr]

  (println attr)
  (println p1)
  (println p2)

  (let [
        p2* (group-by :db/id p2)

        updater (if (am/multiple? am attr)
                  (fn [refs]
                    (let [ids (map :db/id refs)]
                      (for [id ids]
                        (first (get p2* id)))))
                  (fn [ref]
                    (let [{:db/keys [id]} ref]
                      (first (get p2* id)))))
        ]
    (for [p p1]
      (update p attr updater))))


(defn pull-join-backref
  [{:as scope :keys [am]}
   p1 p2 _attr]

  (println _attr)
  (println p1)
  (println p2)

  (let [attr (am/backref->ref _attr)

        ;; fff (group-by
        ;;      (fn [{:keys [e a v]}]
        ;;        [e (when (= a attr) (:db/id v))])
        ;;      p2)

        ;; _ (println fff)

        ;; p2* (group-by (comp :db/id :v) p2)

        ]

    p1

    #_
    (concat p1 (for [[e [row]] p2*
                     :when e]
                 {:e e
                  :a _attr
                  :v row}))))


(defn pull-connect
  [{:as scope :keys [am]}
   p refs* backrefs*]

  #_
  (reduce
   (fn [p [attr pattern]]
     (let [ids (if (am/multiple? am attr)
                 (->> (mapcat attr p)
                      (map :db/id))
                 (map (comp :db/id attr) p))
           {:keys [wc? attrs refs backrefs]}
           (split-pattern pattern)
           p2 (pull* scope ids (when-not wc? attrs))
           p* (pull-connect scope p2 refs backrefs)]
       (pull-join scope p p* attr)))
   p
   refs*)

  (reduce
   (fn [p [_attr pattern]]


     (let [attr (am/backref->ref _attr)
           ids (map :db/id p)

           {:keys [wc? attrs refs backrefs]}
           (split-pattern pattern)

           p2 (pull*-refs scope ids [attr] attrs)
           p* (pull-connect scope p2 refs backrefs)]

       (pull-join-backref scope p p* _attr)))
   p
   backrefs*)

  )


(defn pull-many
  [scope pattern ids]
  (let [{:keys [wc? attrs refs backrefs]}
        (split-pattern pattern)
        p (pull* scope ids (when-not wc? attrs))]
    (pull-connect scope p refs backrefs)))


(defn pull
  [scope pattern id]
  (first (pull-many scope pattern [id])))



;; TODOs
;; limits for backrefs
;; attr aliases



#_
(do
  (pull _scope '[:artist/*] [:db/ident :metallica])
  (pull _scope '[* {:release/artist [*]}] [:db/ident :metallica])

  (clojure.pprint/pprint
   (pull* _scope
          [99998 99999 177]
          [:release/year :release/artist :db/ident])))

#_
(do

  (def _attrs
    [{:db/ident       :artist/name
      :db/valueType   :db.type/string
      :db/cardinality :db.cardinality/one}

     {:db/ident       :artist/release
      :db/valueType   :db.type/ref
      :db/cardinality :db.cardinality/one}

     {:db/ident       :release/artist
      :db/valueType   :db.type/ref
      :db/cardinality :db.cardinality/many
      :db/isComponent true}

     {:db/ident       :release/year
      :db/valueType   :db.type/integer
      :db/cardinality :db.cardinality/one}

     {:db/ident       :release/tag
      :db/valueType   :db.type/string
      :db/cardinality :db.cardinality/many}])

  (def _db
    {:dbtype "postgresql"
     :dbname "test"
     :host "127.0.0.1"
     :user "ivan"
     :password "ivan"})

  (def _scope
    {:am (am/manager _attrs)
     :en (en/engine _db)}))
