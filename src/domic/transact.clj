(ns domic.transact
  (:require
   [clojure.set :as set]

   [domic.util :as util]
   [domic.runtime :as runtime]
   [domic.query-params :as qp]
   [domic.error :as e]
   [domic.attr-manager :as am]
   [domic.engine :as en]
   [domic.pull2 :refer [pull*]]

   [honeysql.core :as sql]))


;; todo
;; resolve lookups
;; check refs, eids, etc
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
        (e/error! "Wrong tx-node: %s" tx-node)))

    {:datoms (persistent! datoms*)
     :tx-fns (persistent! tx-fns*)}))


(defn validate-tx-data
  [{:as scope :keys [am]}
   datoms]

  ;; (println datoms)

  (let [ids* (transient #{})]

    (doseq [[_ e a v] datoms]

      (when-not (am/known? am a)
        (e/error! "Unknown attribute: %s" a))

      (when (real-id? e)
        (conj! ids* e))

      (when (am/ref? am a)
        (conj! ids* v)))

    (if-let [ids (-> ids* persistent! not-empty)]

      (let [pull (pull* scope ids)
            ids-found (->> pull (map :e) set)
            ids-left (set/difference ids ids-found)
            ids-count (count ids-left)]

        (cond
          (> ids-count 1)
          (e/error! "Entities %s are not found"
                    (util/join ids-left))
          (= ids-count 1)
          (e/error! "Entity %s is not found"
                    (first ids-left)))))))


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

        _ (validate-tx-data scope datoms)

        elist
        (set (for [[_ e] datoms :when (real-id? e)] e))

        alist
        (set (for [[_ _ a] datoms] a))]

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
              (e/error! "Not implemented: %s" func))

            :db/cas
            (let [[e a v v-new] args]
              (e/error! "Not implemented: %s" func))

            ;; else
            (e/error-case! func)))

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

              (e/error! "Entity %s not found!" e))

            :db/add
            (cond

              (temp-id? e)
              (let [row {:e (resolve-tmp-id e)
                         :a (add-param a)
                         :v (add-param v)
                         :t t}]
                (conj! to-insert* row))

              (real-id? e)
              (let [p* (get p-ea [e a])]

                (if (am/multiple? am a)

                  (when (or (nil? p*)
                            (not (contains? (set (map :v p*)) v)))
                    (let [row {:e e
                               :a (add-param a)
                               :v (add-param v)
                               :t t}]
                      (conj! to-insert* row)))

                  (if p*
                    (let [[{:keys [id]}] p*]
                      (conj! to-update* {:id id
                                         :v (add-param v)
                                         :t t}))
                    (let [row {:e e
                               :a (add-param a)
                               :v (add-param v)
                               :t t}]
                      (conj! to-insert* row)))))

              :else
              (e/error! "Wrong entity type: %s" e))))

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
                   :set {:v v}
                   :where [:= :id id]}
               @qp)))

          ;; delete
          (when to-delete
            (en/execute-map
             en {:delete-from table
                 :where [:in :id to-delete]}
             @qp)))

        nil))))
