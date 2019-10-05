(ns domic.transact
  (:require
   [clojure.set :as set]

   [domic.runtime :as runtime]
   [domic.query-params :as qp]
   [domic.error :refer [error! error-case!]]
   [domic.attr-manager :as am]
   [domic.engine :as en]
   [domic.pull2 :refer [pull*]]

   [honeysql.core :as sql]))


;; todo
;; resolve lookups
;; write log
;; check history attrib
;; process functions


(defn- temp-id [] (str (gensym "e")))

(def temp-id? string?)

(def real-id? int?)


(defn prepare-tx-data
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
  [{:as scope :keys [table
                     table-trx
                     table-seq
                     en am]}
   tx-data]

  (let [resolve-tmp-id
        (memoize (fn [tmp-e]
                   (runtime/get-new-id scope)))

        qp (qp/params)
        add-param (partial qp/add-alias qp)

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

            t (runtime/get-new-id scope)]

        ;; process tx functions
        (doseq [[func & args] tx-fns]

          (case func
            :db/retractEntity
            (let [[e] args]
              (error! "Not implemented: %s" func))

            :db/cas
            (let [[e a v v-new] args]
              (error! "Not implemented: %s" func))

            ;; else
            (error-case! func)))

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
              (let [row {:e (resolve-tmp-id e)
                         :a (add-param a)
                         :v (add-param v)
                         :t t}]
                (conj! to-insert* row))

              (real-id? e)
              (if-let [p* (get p-ea [e a])]

                (if (am/multiple? am a)

                  (let [vals-old (set (map :v p*))
                        vals-new (set v)
                        vals-add (set/difference vals-new vals-old)]

                    (doseq [v-add vals-add]
                      (let [row {:e e
                                 :a (add-param a)
                                 :v (add-param v-add)
                                 :t t}]
                        (conj! to-insert* row))))

                  (let [[{:keys [id]}] p*]
                    (conj! to-update* {:id id
                                       :v (add-param v)
                                       :t t})))

                (error! "Entity %s not found!" e))

              :else
              (error! "Wrong entity type: %s" e))))

        (let [to-insert (-> to-insert*
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
            (en/execute-map
             en {:insert-into table
                 :columns [:e :a :v :t]
                 :values
                 (mapv (juxt :e :a :v :t) to-insert)}
             @qp))

          ;; update
          (when to-update
            (doseq [{:keys [id v]} to-update]
              (en/execute-map
               en {:update table
                   :set {:v (add-param v)}
                   :where [:= :id id]}
               @qp)))

          ;; delete
          (when to-delete
            (en/execute-map
             en {:delete-from table
                 :where [:in :id to-delete]}
             @qp)))

        nil))))
