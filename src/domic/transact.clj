(ns domic.transact
  (:require
   [domic.sql-helpers :as h]
   [domic.util :as util]
   [domic.runtime :as runtime]
   [domic.query-params :as qp]
   [domic.query-builder :as qb]
   [domic.error :as e]
   [domic.attr-manager :as am]
   [domic.engine :as en]
   [domic.pull2 :as p2]

   [honeysql.core :as sql])
  (:import
   java.util.Date))


;; todo

;; write log
;; check history attrib
;; process functions


(defn fetch-db-ids
  [{:as scope :keys [table-seq
                     en]}
   temp-ids]
  (when (seq temp-ids)
    (let [qb (qb/builder)
          nextval (sql/call :nextval (name table-seq))]
      (doseq [temp-id temp-ids]
        (qb/add-select qb [nextval temp-id]))
      (first
       (en/query en (qb/format qb) {:keywordize? false})))))


(defn collect-temp-ids
  [{:as scope :keys [am]}
   datoms]
  (let [temp-ids* (transient #{})]
    (doseq [[_ e a v] datoms]
      (when (h/temp-id? e)
        (conj! temp-ids* e))
      (when (and (am/ref? am a) (h/temp-id? v))
        (conj! temp-ids* v)))
    (-> temp-ids* persistent! not-empty)))


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
                    (h/temp-id))]

          (doseq [[a v] (dissoc tx-node :db/id)]
            (if (am/multiple? am a)

              (if (coll? v)
                (doseq [v* v]
                  (conj! datoms* [:db/add e a v*]))
                (e/error! (str "Attribute %s is of cardinality/many, "
                               "but its value is not a collection: %s")
                          a v))

              (conj! datoms* [:db/add e a v]))))

        :else
        (e/error! "Wrong tx-node: %s" tx-node)))

    {:datoms (persistent! datoms*)
     :tx-fns (persistent! tx-fns*)}))


(defn collect-ident-info
  [{:as scope :keys [am]}
   datoms]

  (let [ids* (transient #{})
        avs* (transient #{})]

    (doseq [[_ e a v] datoms]

      ;; E
      (cond
        (h/real-id? e)
        (conj! ids* e)

        (h/lookup? e)
        (conj! avs* e)

        (h/ident-id? e)
        (conj! avs* [:db/ident e]))

      ;; V (refs)
      (when (am/ref? am a)

        (cond
          (h/real-id? v)
          (conj! ids* v)

          (h/lookup? v)
          (conj! avs* v)

          (h/ident-id? v)
          (conj! avs* [:db/ident v])))

      ;; A (unique)
      (when (am/unique? am a)
        (conj! avs* [a v])))

    [(persistent! ids*) (persistent! avs*)]))


(defn pull-idents
  [{:as scope :keys [am]}
   ids avs]
  (p2/pull*-idents scope ids avs))


(defn fix-datoms
  [{:as scope :keys [am]}
   datoms pull]

  (let [pull-es (set (map :e pull))
        pull-av (group-by (juxt :a :v) pull)

        get-e!
        (fn [e]
          (or (get pull-es e)
              (e/error! "Cannot resolve id %s" e)))

        get-av
        (fn [av]
          (:e (first (get pull-av av))))

        get-av!
        (fn [av]
          (or (get-av av)
              (e/error! "Cannot find lookup %s" av)))

        -cache (atom {})
        cset! (fn [k v]
                (swap! -cache
                       (fn [*cache]
                         (when-let [v* (get *cache k)]
                           (when (not= v* v)
                             (e/error!
                              "Attempt to redefine temp-id %s from %s to %s"
                              k v* v)))
                         (assoc *cache k v))))
        cswap (fn [k] (get @-cache k k))

        datoms* (transient [])
        _
        (doseq [datom datoms]

          (let [[op e a v] datom

                ;; resolve E
                e (cond
                    (h/real-id? e)
                    (get-e! e)

                    (h/lookup? e)
                    (get-av! e)

                    (h/ident-id? e)
                    (get-av! [:db/ident e])

                    :else e)

                ;; resolve V
                v (cond

                    (am/ref? am a)
                    (cond
                      (h/real-id? v)
                      (get-e! v)

                      (h/lookup? v)
                      (get-av! v)

                      (h/ident-id? v)
                      (get-av! [:db/ident v])

                      :else v)
                    :else v)]

            (conj! datoms* [op e a v])

            ;; unique/ident
            (when (am/unique-identity? am a)
              (when-let [e* (get-av [a v])]
                (when (h/temp-id? e)
                  (cset! e e*))
                (when (and (h/real-id? e) (not= e e*))
                  (e/error! "Ident conflict: AV [%s %s] => %s, but E is %s"
                            a v e* e))))

            ;; unique value
            (when (am/unique-value? am a)
              (when-let [e* (get-av [a v])]
                (when (or (h/temp-id? e)
                          (and (h/real-id? e)
                               (not= e e*)))
                  (e/error! (str "Value conflict: entity with AV [%s %s] "
                                 "already exists (%s)")
                            a v e*))))))

        datoms* (persistent! datoms*)]

    (if (empty? @-cache)
      datoms*
      (doall (for [[op e a v] datoms*]
               [op (cswap e) a v])))))


(defn validate-attrs!
  [{:as scope :keys [am]}
   datoms]
  (let [attrs (map #(get % 2) datoms)]
     (am/validate-many! am attrs)))


(defn transact
  [{:as scope :keys [table
                     table-trx
                     table-seq
                     en am]}
   tx-data]

  (let [qp (qp/params)
        add-param (partial qp/add-alias qp)

        t-inst (new Date)
        t-id   -1

        to-insert* (transient [])
        to-delete* (transient [])
        to-update* (transient [])

        {:keys [datoms tx-fns]}
        (prepare-tx-data scope tx-data)

        [ids avs] (collect-ident-info scope datoms)]

    (en/with-tx [en en]

      (let [scope    (assoc scope :en en)

            pull     (pull-idents scope ids avs)
            _        (validate-attrs! scope datoms)
            datoms   (fix-datoms scope datoms pull)
            temp-ids (collect-temp-ids scope datoms)
            t-tmp-id (str (gensym "tx"))
            temp-ids (conj temp-ids t-tmp-id)
            -ids-map   (fetch-db-ids scope temp-ids)
            ->db-id  (fn [temp-id]
                       (or (get -ids-map temp-id)
                           (e/error! "Cannot resolve temp-id: %s" temp-id)))
            t        (->db-id t-tmp-id)
            p-ea     (group-by (juxt :e :a) pull)]

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

              (h/temp-id? e)
              (let [row {:e (->db-id e)
                         :a (add-param a)
                         :v (add-param v)
                         :t t}]
                (conj! to-insert* row))

              (h/real-id? e)
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

          ;; transaction record
          (en/execute-map
           en {:insert-into table
               :values [{:e t
                         :a (add-param :db/txInstant)
                         :v (add-param t-inst)
                         :t (add-param t-id)}]}
           @qp)

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
