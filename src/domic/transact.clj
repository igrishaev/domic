(ns domic.transact
  (:require
   [clojure.set :as set]

   [domic.const :as const]
   [domic.sql-helpers :as h]
   [domic.util :as util]
   [domic.runtime :as rt]
   [domic.query-params :as qp]
   [domic.query-builder :as qb]
   [domic.error :as e]
   [domic.attr-manager :as am]
   [domic.engine :as en]
   [domic.pull :as p]

   [honeysql.core :as sql])
  (:import
   java.util.Date))


;; todo
;; retraction
;; check history attrib
;; process functions

#_
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

#_
(defn collect-datoms-info
  [{:as scope :keys [am]}
   datoms]

  (let [ids* (transient #{})
        avs* (transient #{})
        tmp* (transient #{})]

    (doseq [[_ e a v] datoms]

      ;; E
      (cond

        (h/temp-id? e)
        (conj! tmp* e)

        (h/real-id? e)
        (conj! ids* e)

        (h/lookup? e)
        (conj! avs* e)

        (h/ident-id? e)
        (conj! avs* [:db/ident e]))

      ;; V (refs)
      (when (am/ref? am a)

        (cond

          (h/temp-id? v)
          (conj! tmp* v)

          (h/real-id? v)
          (conj! ids* v)

          (h/lookup? v)
          (conj! avs* v)

          (h/ident-id? v)
          (conj! avs* [:db/ident v])))

      ;; A (unique)
      (when (am/unique? am a)
        (conj! avs* [a v])))

    [(persistent! ids*)
     (persistent! avs*)
     (persistent! tmp*)]))

#_
(defn pull-idents
  [{:as scope :keys [am]}
   ids avs]
  (p/-pull*-idents scope ids avs))

#_
(defn fix-datoms
  [{:as scope :keys [am]}
   datoms pull fn-id]

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

                    (h/temp-id? e)
                    (fn-id e)

                    (h/real-id? e)
                    (get-e! e)

                    (h/lookup? e)
                    (get-av! e)

                    (h/ident-id? e)
                    (get-av! [:db/ident e])

                    :else
                    (e/error! "Wrong e: %s" e))

                ;; resolve V
                v (cond

                    (am/ref? am a)
                    (cond

                      (h/temp-id? v)
                      (fn-id v)

                      (h/real-id? v)
                      (get-e! v)

                      (h/lookup? v)
                      (get-av! v)

                      (h/ident-id? v)
                      (get-av! [:db/ident v])

                      :else
                      (e/error! "Wrong v: %s" v))

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

#_
(defn validate-attrs!
  [{:as scope :keys [am]}
   datoms]
  (let [attrs (mapv #(get % 2) datoms)]
     (am/validate-many! am attrs)))

#_
(defn transact
  [{:as scope :keys [table
                     table-log
                     en am]}
   tx-data]

  (let [qp (qp/params)
        add-param (partial qp/add-alias qp)

        t-inst (new Date)
        t-id   const/tx-id

        to-log*    (transient [])
        to-insert* (transient [])
        to-delete* (transient [])
        to-update* (transient [])

        add-log (fn [row] (conj! to-log* row))

        {:keys [datoms tx-fns]}
        (prepare-tx-data scope tx-data)

        _ (validate-attrs! scope datoms)

        [ids avs temp-ids] (collect-datoms-info scope datoms)

        t-tmp-id (str (gensym "tx"))
        temp-ids (conj temp-ids t-tmp-id)]

    (en/with-tx [en en]

      (let [scope    (assoc scope :en en)

            -ids-map (rt/allocate-db-ids scope temp-ids)
            pull     (pull-idents scope ids avs)

            fn-id    (fn [temp-id]
                       (or (get -ids-map temp-id)
                           (e/error! "Cannot resolve temp-id: %s"
                                     temp-id)))

            datoms   (fix-datoms scope datoms pull fn-id)

            t        (fn-id t-tmp-id)

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

            #_
            :db/retract
            #_
            (if (h/real-id? e)

              (do

                (add-log {:e e
                          :a (add-param a)
                          :v (add-param v)
                          :t t
                          :op false})

                (if-let [p* (get p-ea [e a])]
                  (doseq [{:keys [id] v* :v} p*
                          :when (= v v*)]
                    (conj! to-delete* id))
                  (e/error! "Entity %s not found!" e)))

              (e/error! "Cannot retract entity %s" e))

            :db/add
            (if-let [p* (get p-ea [e a])]

              ;; found in pull
              (if (am/multiple? am a)

                ;; multiple
                (if (contains? (mapv :v p*) v)

                  ;; found in set; do nothing
                  nil

                  ;; not found in set; insert new
                  (let [row {:e e
                             :a (add-param a)
                             :v (add-param v)
                             :t t}]
                    (conj! to-insert* row)
                    (add-log (assoc row :op true))))

                ;; not multiple; update existing
                (let [[{:keys [id]}] p*]
                  (conj! to-update* {:id id
                                     :v (add-param v)
                                     :t t})
                  (add-log {:e e
                            :a (add-param a)
                            :v (add-param v)
                            :t t
                            :op true})))

              ;; not found in pull; insert new
              (let [row {:e e
                         :a (add-param a)
                         :v (add-param v)
                         :t t}]
                (conj! to-insert* row)
                (add-log (assoc row :op true))))))

        ;; insert transaction
        (let [row {:e t
                   :a (add-param :db/txInstant)
                   :v (add-param t-inst)
                   :t (add-param t-id)}]
          (conj! to-insert* row))

        ;; database unsert/update
        (let [to-log (-> to-log*
                         persistent!
                         not-empty)

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
             @qp))

          ;; log
          (when to-log
            (en/execute-map
             en {:insert-into table-log
                 :values to-log}
             @qp)))

        nil))))


(def into-map (partial into {}))


(defn- resolve*
  [scope param]
  (cond

    (h/lookup? param)
    (rt/resolve-lookup! scope param)

    (h/ident-id? param)
    (rt/resolve-lookup! scope [:db/ident param])

    ;; (h/temp-id? param)

    :else param))


(defn transact
  [{:as scope :keys [am en
                     table]}
   tx-maps]

  (let [t (rt/allocate-id scope)

        qp (qp/params)
        add-param (partial qp/add-alias qp)

        ]

    (doseq [tx-map tx-maps]

      (let [to-insert* (transient [])
            +insert (fn [& args]
                      (conj! to-insert* (mapv add-param args)))

            to-update* (transient [])
            +update (fn [& args]
                      (conj! to-update* (mapv add-param args)))


            tx-map (into-map
                    (for [[a v] tx-map]
                      (if (or (= a :db/id)
                              (am/ref? am a))
                        [a (resolve* scope v)]
                        [a v])))

            e (or (get tx-map :db/id)
                  (str (gensym "id")))

            tx-map (dissoc tx-map :db/id)

            ;; unique-pairs
            ;; (for [[a v] tx-map]

            ;;   (when (am/unique? am a)
            ;;     (when-let [e* (resolve* scope v scope [a v])]
            ;;       (cond

            ;;         (am/unique-identity? am a)
            ;;         [e* :identity]

            ;;         (am/unique-value? am a)
            ;;         [e* :value]

            ;;         :else
            ;;         (e/error! "Wrong unique attribute: %s" a)))))

            ;; unique-map (into-map (filter some? unique-pairs))

            ;; eids (-> (keys unique-map)
            ;;          (concat (when (h/real-id? e) [e]))
            ;;          (set))

            ;; _ (when (> (count eids) 1)
            ;;     (e/error! "Uniqueness conflict: %s" tx-map))

            ;; e (cond

            ;;     (and (h/temp-id? e) (seq unique-map))
            ;;     (ffirst unique-map)

            ;;     :else e)

            ]

        (cond

          (h/temp-id? e)
          (let [e* (rt/allocate-id scope)]
            (doseq [[a v] tx-map]
              (if (am/multiple? am a)
                (doseq [v v]
                  (+insert e* a v t))
                (+insert e* a v t))))

          (h/real-id? e)
          ;; transaction
          ;; for update
          (let [p (p/pull scope '[*] e)]
            (doseq [[a v] tx-map]

              (if (contains? p a)

                (if (am/multiple? am a)

                  (let [v-new (set v)
                        v-old (set (get p a))
                        v-add (set/difference v-new v-old)]
                    (doseq [v* v-add]
                      (+insert e a v* t)))

                  (+update e a (get p a) v))

                (if (am/multiple? am a)
                  (doseq [v v]
                    (+insert e a v t))
                  (+insert e a v t)))))

          :else (e/error! "Weird id: %s" e))

        (when-let [to-insert (-> to-insert* persistent! seq)]
          (en/execute-map
           en {:insert-into table
               :columns [:e :a :v :t]
               :values to-insert}
           @qp))

        (when-let [to-update (-> to-update* persistent! seq)]
          (doseq [[e a v v*] to-update]
            (en/execute-map
             en {:update table
                 :set {:v v*}
                 :where [:and [:= :e e] [:= :a a] [:= :v v]]}
             @qp))))

      ))

  )
