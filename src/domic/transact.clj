(ns domic.transact
  (:require

   ;; [clojure.spec.alpha :as s]

   ;; [domic.runtime :refer [resolve-lookup!]]
   [domic.util :refer
    [kw->str

     ;; drop-nils sym-generator

     ]]

   [domic.error :refer [error!]]
   ;; [domic.query-builder :as qb]
   ;; [domic.query-params :as qp]

   [domic.attr-manager :as am]
   [domic.engine :as en]

   ;; [domic.sql-helpers :refer
   ;;  [->cast lookup?]]

   [honeysql.core :as sql]
   ;; [datomic-spec.core :as ds]

   ))


(defn maps->list
  [maps]
  (let [result* (transient [])]
    (doseq [map maps]
      (let [e (or (:db/id map)
                  (str (gensym "e")))]
        (doseq [[a v] (dissoc map :db/id)]
          (conj! result* [:db/add e a v]))))
    (persistent! result*)))


(defn- next-id
  [{:as scope :keys [en]}]
  (let [query ["select nextval(?) as id" "_foo"]]
    (-> (en/query en query)
        first
        :id)))


#_
(defn- map-new?
  [mapping]
  (not (some-> mapping :db/id)))


(defn- map->e
  [mapping]
  (some-> mapping :db/id))


#_
(defn- map->rows
  [e mapping t]
  (for [[a v] mapping]
    {:e e :a a :v v :t t}))


#_
(defn- process-maps-new
  [scope
   maps-new]
  )


(defn aaa
  [{:as scope :keys [en]}
   vals-old
   vals-new
   pg-type]

  (let [foo {:select :v
             :from [{:values [1 2 3]}]
             :where [:not [:in :v [5 6 7]]]}])







  )



(defn transact
  [{:as scope :keys [en am]}
   mappings]

  ;; with transaction

  (let [t (next-id scope)

        temp->e (fn [_]
                  (next-id scope))

        #_
        (let [cache (atom {})]
          (fn [temp-e]
            (or (get @cache temp-e)
                (let [id (next-id scope)]
                  (swap! cache assoc temp-e id)
                  id))))

        lists (maps->list mappings)

        es
        (for [[_ e a v] lists
              :when (int? e)]
          e)

        attrs
        (for [[_ e a v] lists]
          (kw->str a))

        query (when (and (not-empty es)
                         (not-empty attrs))
                (sql/format
                 {:select [:*]
                  :from [:datoms4]
                  :where [:and
                          [:in :e (set es)]
                          [:in :a (set attrs)]]}))

        datoms (when query
                 (en/query en query))

        datoms-ea
        (->> datoms
             (map (fn [datom]
                    (update datom :a keyword)))
             (group-by (juxt :e :a)))

        _ (println (keys datoms-ea))

        to-insert* (transient [])
        to-delete* (transient [])
        to-update* (transient [])]

    (doseq [[op e a v] lists]
      (case op

        ;; :db/retract
        ;; (conj! to-delete* )

        :db/add
        (cond

          ;; todo inline nextval
          (string? e)
          (let [vm (if (am/multiple? am a)
                     v [v])]
            (doseq [v* vm]
              (let [row {:e (temp->e e) :a a :v v* :t t}]
                (conj! to-insert* row))))

          (int? e)
          (if-let [datoms* (get datoms-ea [e a])]

            (if (am/multiple? am a)

              (let [vm v]
                1

                #_
                (conj! to-update* {:id id :v v}))

              (let [[{:keys [id]}] datoms*]
                (conj! to-update* {:id id :v v})))

            (error! "Entity %s not found!" e))

          :else
          (error! "Wrong entity type: %s" e))))

    (persistent! to-insert*)
    #_(persistent! to-update*)

    #_
    (en/insert-multi en :datoms4 rows-new)))

#_
(do

  (def _attrs
    [{:db/ident       :artist/name
      :db/valueType   :db.type/string
      :db/cardinality :db.cardinality/one}

     {:db/ident       :release/artist
      :db/valueType   :db.type/ref
      :db/cardinality :db.cardinality/one
      :db/isComponent true}

     {:db/ident       :release/year
      :db/valueType   :db.type/integer
      :db/cardinality :db.cardinality/many}

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
