(ns domic.transact
  (:require
   [clojure.set :as set]

   [domic.error :refer [error!]]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]
   [domic.pull2 :refer [pull*]]))


;; todo
;; resolve lookups
;; entity ids for sqlite?
;; write log


(defn- temp-id [] (str (gensym "e")))

(def temp-id? string?)

(def real-id? int?)


(defn- prepare-tx-data
  [{:as scope :keys [am]}
   tx-data]
  (let [datoms* (transient [])
        tx-fns* (transient [])]

    (doseq [tx-node tx-data]

      (cond
        (vector? tx-node)
        (let [[op] tx-node]
          (case op
            (:db/add :db/retract)
            (conj! datoms* tx-node)
            ;; else
            (conj! tx-fns* tx-node)))

        (map? tx-node)
        (let [e (or (:db/id tx-node)
                    (temp-id))]
          (doseq [[a v] (dissoc tx-node :db/id)]
            (if (am/multiple? am a)
              (doseq [v* v]
                (conj! datoms* [:db/add e a v*]))
              (conj! datoms* [:db/add e a v]))))

        :else
        (error! "Wrong tx-node: %s" tx-node)))

    {:datoms (persistent! datoms*)
     :tx-fns (persistent! tx-fns*)}))


(defn transact
  [{:as scope :keys [en am]}
   tx-data]

  (let [qp (qp/params)
        add-alias (partial qp/add-alias qp)

        to-insert* (transient [])
        to-delete* (transient [])
        to-update* (transient [])

        {:keys [datoms tx-fns]}
        (prepare-tx-data scope tx-data)

        elist
        (for [[_ e] datoms :when (real-id? e)] e)

        alist
        (for [[_ _ a] datoms] a)]

    (en/with-tx [en en]

      (let [scope (assoc scope :en en)

            p (when (and (not-empty elist)
                         (not-empty alist))
                (pull* scope elist alist))

            p-ea (group-by (juxt :e :a) p)

            e-get (let [cache (atom {})]
                    (fn [e-tmp]
                      (or (get @cache e-tmp)
                          (let [e-new (rand-int 999999999)] ;; todo
                            (swap! cache assoc e-tmp e-new)
                            e-new))))

            t (rand-int 999999999)] ;; todo

        ;; process tx functions
        (doseq [[func & args] tx-fns]

          (case func

            :db/retractEntity
            (let [[e] args] ;; todo
              1)

            :db/cas
            (let [[e a v v-new] args] ;; todo
              2)))

        ;; process add/retract
        (doseq [[op e a v] datoms]
          (case op

            :db/retract
            (if-let [p* (get p-ea [e a])]
              (let [ids (for [{:keys [id] v* :v} p*
                              :when (= v v*)]
                          id)]
                (doseq [id ids]
                  (conj! to-delete* id)))

              (error! "Entity %s not found!" e))

            :db/add
            (cond

              (temp-id? e)
              (let [vm (if (am/multiple? am a)
                         v [v])]
                (doseq [v* vm]
                  (let [row {:e (e-get e)
                             :a (add-alias a)
                             :v (add-alias v*)
                             :t t}]
                    (conj! to-insert* row))))

              (real-id? e)
              (if-let [p* (get p-ea [e a])]

                (if (am/multiple? am a)

                  (let [vals-old (set (map :v p*))
                        vals-new (set v)
                        vals-add (set/difference vals-new vals-old)]

                    (doseq [v-add vals-add]
                      (let [row {:e e
                                 :a (add-alias a)
                                 :v (add-alias v-add)
                                 :t t}]
                        (conj! to-insert* row))))

                  (let [[{:keys [id]}] p*]
                    (conj! to-update* {:id id
                                       :v (add-alias v)
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
                            :values
                            (map (juxt :e :a :v :t) to-insert)}
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
            (en/execute en {:delete-from :datoms4
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
