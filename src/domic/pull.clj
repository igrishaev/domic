(ns domic.pull
  (:require
   [domic.attributes :as at]
   [domic.error :as e]
   [domic.query-builder :as qb]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]
   [domic.sql-helpers :as h]

   [honeysql.core :as sql])

  (:import
   java.sql.ResultSet))

;; todo
;; limits for backrefs
;; attr aliases
;; try weird attrs


(def WC '*)

(def conj-set (fnil conj #{}))


(defn- rs->datom
  [{:as scope :keys [am]}
   ^ResultSet rs]
  (let [attr (keyword (.getString rs 3))
        attr-type (am/get-type am attr)]
    {:e (.getLong rs 2)
     :a attr
     :v (at/rs->clj attr-type rs 4)
     :t (.getLong rs 5)}))


(defn- rs->datoms
  [scope]
  (fn [^ResultSet rs]
    (let [result* (transient [])]
      (while (.next rs)
        (conj! result* (rs->datom scope rs)))
      (persistent! result*))))


(defn- rs->maps
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


(defn- split-pattern
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
        (if (at/attr-backref? pattern)
          (assoc! backrefs* pattern [WC])
          (conj! attrs* pattern))


        (map? pattern)
        (doseq [[attr pattern] pattern]
          (if (at/attr-backref? attr)
            (assoc! backrefs* attr pattern)
            (assoc! refs* attr pattern)))

        (= pattern WC) nil
        (= pattern (str WC)) nil

        :else
        (e/error! "Wrong pattern: %s" pattern)))

    {:wc? wc?
     :attrs (persistent! attrs*)
     :refs (persistent! refs*)
     :backrefs (persistent! backrefs*)}))


(defn- pull*
  [{:as scope :keys [en table]}
   ids & [attrs]]

  (when (seq ids)

    (let [qp (qp/params)
          add-param (partial qp/add-alias qp)

          ids*   (mapv add-param ids)
          attrs* (mapv add-param attrs)

          sql (sql/build
               :select :*
               :from table
               :where [:and
                       [:in :e ids*]
                       (when (seq attrs)
                         [:in :a attrs*])])

          query (sql/format sql @qp)]

      (en/query-rs en query (rs->maps scope)))))


(defn- pull*-refs
  [{:as scope :keys [en am table]}
   ids-ref attrs-ref & [attrs]]

  (println "backrefs" ids-ref attrs-ref attrs)

  (let [qp (qp/params)
        add-param (partial qp/add-alias qp)

        ids-ref*   (mapv add-param ids-ref)
        attrs-ref* (mapv add-param attrs-ref)
        attrs*     (mapv add-param attrs)

        db-type (am/db-type am :db.type/ref)
        v-cast (h/->cast :v db-type)

        sub (sql/build
             :select :e
             :from table
             :where [:and
                     [:in v-cast ids-ref*]
                     [:in :a attrs-ref*]])

        sql (sql/build
             :select :*
             :from table
             :where [:and
                     [:in :e sub]
                     (when (seq attrs)
                       [:in :a attrs*])])

        query (sql/format sql @qp)]

    (en/query-rs en query (rs->maps scope))))


(defn- pull-join
  [{:as scope :keys [am]}
   p1 p2 attr]

  (let [p2* (group-by :db/id p2)

        updater (if (am/multiple? am attr)
                  (fn [refs]
                    (let [ids (map :db/id refs)]
                      (for [id ids]
                        (first (get p2* id)))))
                  (fn [ref]
                    (let [{:db/keys [id]} ref]
                      (first (get p2* id)))))]
    (for [p p1]
      ;; todo check if got the result
      (update p attr updater))))


(defn- group-pull-backref
  [p m? attr]
  (let [coll (if m?
               (let [tmp* (transient [])]
                 (doseq [p p]
                   (let [ids (get p attr)]
                     (doseq [id ids]
                       (conj! tmp* (assoc p attr id)))))
                 (persistent! tmp*))
               p)]
    (group-by (comp :db/id attr) coll)))


(defn- pull-join-backref
  [{:as scope :keys [am]}
   p1 p2 _attr]

  (let [attr (at/attr-backref->ref _attr)
        regr* (transient {})
        m? (am/multiple? am attr)
        grouped (group-pull-backref p2 m? attr)]

    (for [p p1]
      (assoc p _attr (get grouped (:db/id p))))))


(defn- pull-connect
  [{:as scope :keys [am]}
   p refs* backrefs*]

  (reduce
   (fn [p [_attr pattern]]

     (let [attr (at/attr-backref->ref _attr)
           ids (map :db/id p)

           {:keys [wc? attrs refs backrefs]}
           (split-pattern pattern)

           p2 (pull*-refs scope ids [attr] attrs)
           p* (pull-connect scope p2 refs backrefs)]

       (pull-join-backref scope p p* _attr)))

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

   backrefs*))


(defn pull-many
  [scope pattern ids]
  (let [{:keys [wc? attrs refs backrefs]}
        (split-pattern pattern)
        p (pull* scope ids (when-not wc? attrs))]
    (pull-connect scope p refs backrefs)))


(defn pull
  [scope pattern id]
  (first (pull-many scope pattern [id])))


(defn -pull*-idents
  "
  Special pull for transact module
  "
  [{:as scope :keys [table
                     en am]}
   & [eids av-pairs]]
  (let [qp (qp/params)
        add-param (partial qp/add-alias qp)
        ors* (transient [])]

    (when (seq eids)
      (conj! ors* [:in :e (mapv add-param eids)]))

    (doseq [[a v] av-pairs]

      (let [db-type (am/db-type am a)]

        (if (h/lookup? v)

          (let [[a* v*] v
                db-type* (am/db-type am a*)

                sub
                {:select [:e]
                 :from [table]
                 :where [:and
                         [:= :a (add-param a*)]
                         [:= (h/->cast :v db-type*)
                          (add-param v*)]]
                 :limit (sql/inline 1)}]

            (conj! ors*
                   [:and
                    [:= :a (add-param a*)]
                    [:= (h/->cast :v db-type*) (add-param v*)]])

            (conj! ors*
                   [:and
                    [:= :a (add-param a)]
                    [:= (h/->cast :v db-type) sub]]))

          (conj! ors*
                 [:and
                  [:= :a (add-param a)]
                  [:= (h/->cast :v db-type) (add-param v)]]))))

    (when-let [ors (-> ors* persistent! not-empty)]
      (let [sql (sql/build :select :*
                           :from table
                           :where (into [:or] ors))
            query (sql/format sql @qp)]
        (en/query-rs en query (rs->datoms scope))))))


(defn -pull-attrs
  [{:as scope :keys [table
                     en am]}]

  (let [qp (qp/params)
        add-param (partial qp/add-alias qp)

        sub (sql/build
             :select :e
             :from table
             :where [:= :a (add-param :db/valueType)])

        sql (sql/build
             :select :*
             :from table
             :where [:in :e sub])

        query (sql/format sql @qp)]

    (en/query-rs en query (rs->maps scope))))


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
