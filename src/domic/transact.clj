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


(defn validate-attrs!
  [{:as scope :keys [am]}
   tx-maps]

  (let [attrs
        (-> (mapcat keys tx-maps)
            (set)
            (disj :db/id))]
    (am/validate-many! am attrs)))


(def into-map (partial into {}))


(defn transact
  [{:as scope :keys [am en
                     table]}
   tx-maps]

  (validate-attrs! scope tx-maps)

  (en/with-tx [en en]

    (let [scope (assoc scope :en en)

          t (rt/allocate-id scope)

          qp (qp/params)
          add-param (partial qp/add-alias qp)

          id-cache (atom {})

          ;; strict resolve (throws an error)
          resolve-ref!
          (fn [v]
            (cond

              (h/lookup? v)
              (rt/resolve-lookup! scope v)

              (h/ident-id? v)
              (rt/resolve-lookup! scope [:db/ident v])

              (h/temp-id? v)
              (or (get @id-cache v)
                  (e/error! "Temp id `%s` cannot be resolved!" v))

              :else v))

          resolve-unique
          (fn [a v]
            (rt/resolve-lookup scope [a v]))

          ;; non-strict resolve
          resolve-e!
          (fn [v]
            (cond

              (h/lookup? v)
              (rt/resolve-lookup! scope v)

              (h/ident-id? v)
              (rt/resolve-lookup! scope [:db/ident v])

              (h/temp-id? v)
              (get @id-cache v v)

              :else v))

          ]

      (doseq [tx-map tx-maps]

        (let [to-insert* (transient [])
              +insert (fn [& args]
                        (conj! to-insert* (mapv add-param args)))

              to-update* (transient [])
              +update (fn [& args]
                        (conj! to-update* (mapv add-param args)))

              e (or (get tx-map :db/id)
                    (str (gensym "id")))

              ;; try to resolve e
              e (resolve-e! e)

              tx-map (dissoc tx-map :db/id)

              ;; resolve lookups and idents in tx-map
              ;; for refs, act strictly
              tx-map (into-map
                      (for [[a v] tx-map]
                        (if (am/ref? am a)
                          (if (am/multiple? am a)
                            [a (mapv resolve-ref! v)]
                            [a (resolve-ref! v)])
                          [a v])))

              unique-pairs
              (for [[a v] tx-map]

                (when (am/unique? am a)
                  (when-let [e* (resolve-unique a v)]
                    (cond

                      (am/unique-identity? am a)
                      [e* :identity]

                      (am/unique-value? am a)
                      [e* :value]

                      :else
                      (e/error! "Wrong unique attribute: %s" a)))))

              unique-map (into-map (filter some? unique-pairs))

              eids (-> (keys unique-map)
                       (concat (when (h/real-id? e) [e]))
                       (set))

              _ (when (> (count eids) 1)
                  (e/error! "Uniqueness conflict: %s" tx-map))

              e (cond

                  (and (h/temp-id? e) (seq unique-map))
                  (let [e* (ffirst unique-map)]

                    ;; check unique type
                    (swap! id-cache assoc e e*)

                    ;; when ident => get this id
                    ;; when value => raise error
                    e*)

                  :else e)

              ]

          (cond

            (h/temp-id? e)
            (let [e* (rt/allocate-id scope)]

              ;; set cache for tempid
              (swap! id-cache assoc e e*)

              (doseq [[a v] tx-map]
                (if (am/multiple? am a)
                  (doseq [v v]
                    (+insert e* a v t))
                  (+insert e* a v t))))

            (h/real-id? e)
            ;; for update
            (let [p (p/pull scope '[*] e)

                  _ (when-not p
                      (e/error! "Entity %s is not found" e))

                  ;; turn ref maps into integers
                  p (into-map (for [[a v] p]
                                (if (am/ref? am a)
                                  [a (:db/id v)]
                                  [a v])))]

              (doseq [[a v] tx-map]

                (if (contains? p a)

                  (if (am/multiple? am a)

                    (let [v-new (set v)
                          v-old (set (get p a))
                          v-add (set/difference v-new v-old)]
                      (doseq [v* v-add]
                        (+insert e a v* t)))

                    (let [v* (get p a)]
                      (when-not (= v v*)
                        (+update e a v* v))))

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

        )))

  )
