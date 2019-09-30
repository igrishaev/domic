(ns domic.transact
  (:require

   [clojure.set :as set]

   [domic.util :refer
    [kw->str sym-generator]]
   [domic.error :refer [error!]]
   [domic.query-builder :as qb]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]

   [domic.pull :refer [pull*]]

   [domic.sql-helpers :refer [->cast]]

   [honeysql.core :as sql]))


(def seq-name "_foo")


(defn add-param
  [qp value]
  (let [alias (gensym "param")
        param (sql/param alias)]
    (qp/add-param qp alias value)
    param))


(defn- maps->list
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
  (let [query ["select nextval(?) as id" seq-name]]
    (-> (en/query en query)
        first
        :id)))


(defn transact
  [{:as scope :keys [en am]}
   mappings]

  (let [qp (qp/params)
        add-param* (partial add-param qp)

        to-insert* (transient [])
        to-delete* (transient [])
        to-update* (transient [])

        lists (maps->list mappings)

        elist
        (for [[_ e a v] lists
              :when (int? e)]
          e)

        alist
        (for [[_ e a v] lists]
          (kw->str a))]

    (en/with-tx [en en]

      (let [scope (assoc scope :en en)

            p (when (and (not-empty elist)
                         (not-empty alist))
                (pull* scope elist alist))

            p-ea (group-by (juxt :e :a) p)

            e-get (let [cache (atom {})]
                    (fn [e-tmp]
                      (or (get @cache e-tmp)
                          (let [e-new (rand-int 999999999)]
                            (swap! cache assoc e-tmp e-new)
                            e-new))))

            t (next-id scope)]

        (doseq [[op e a v] lists]
          (case op

            ;; :db/retract
            ;; (conj! to-delete* )

            :db/add
            (cond

              (string? e)
              (let [vm (if (am/multiple? am a)
                         v [v])]
                (doseq [v* vm]
                  (let [row {:e (e-get e)
                             :a (add-param* a)
                             :v (add-param* v*)
                             :t t}]
                    (conj! to-insert* row))))

              (int? e)
              (if-let [p* (get p-ea [e a])]

                (if (am/multiple? am a)

                  (let [vals-old (set (map :v p*))
                        vals-new (set v)
                        vals-add (set/difference vals-new vals-old)]

                    (doseq [v-add vals-add]
                      (let [row {:e e
                                 :a (add-param* a)
                                 :v (add-param* v-add)
                                 :t t}]
                        (conj! to-insert* row))))

                  (let [[{:keys [id]}] p*]
                    (conj! to-update* {:id id
                                       :v (add-param* v)
                                       :t t})))

                (error! "Entity %s not found!" e))

              :else
              (error! "Wrong entity type: %s" e))))

        (let [params (qp/get-params qp)

              to-insert (-> to-insert*
                            persistent!
                            not-empty)

              to-update (-> to-update*
                            persistent!
                            not-empty)

              to-delete (-> to-delete*
                            persistent!
                            not-empty)]

          ;; insert
          (when to-insert
            (en/execute en {:insert-into :datoms4
                            :columns [:e :a :v :t]
                            :values (map (juxt :e :a :v :t) to-insert)}
                        params))

          ;; update
          (when to-update
            (doseq [{:keys [id v]} to-update]
              (en/execute en {:update :datoms4
                              :set {:v v}
                              :where [:= :id id]}
                          params)))

          ;; delete
          (when to-delete
            (en/execute en {:delete [:datoms4]
                            :where [:in :id to-delete]}
                        params)))

        nil))))


#_
(do

  (transact _scope
            [{:db/id 336943376
              :release/artist 999
              ;; :release/year 333
              ;; :release/tag ["dd0" "bbb0"]
              }]))

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
