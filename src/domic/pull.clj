(ns domic.pull
  (:require
   [clojure.spec.alpha :as s]

   [domic.util :refer [kw->str]]
   [domic.error :refer [error!]]
   [domic.query-builder :as qb]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en]

   [honeysql.core :as sql]
   [datomic-spec.core :as ds]))


(defn- qb-filter*
  [qb mapping]
  (doseq [[field value] mapping]
    (cond
      (coll? value)
      (qb/add-where qb [:in field value])
      (some? value)
      (qb/add-where qb [:= field value])
      :else
      (error! "Unknown filter"))))


(defn- resolve-attrs
  [{:as scope :keys [en]}
   mapping]

  (let [qb (qb/builder)
        qb* (qb/builder)]

    (qb-filter* qb mapping)

    (qb/add-select qb :e)
    (qb/add-from qb :datoms4)

    (qb/add-select qb* :a)
    (qb/add-from qb* :datoms4)
    (qb/add-where qb* [:in :e (qb/->map qb)])

    (let [query (qb/format qb*)
          result (en/query en query {:as-arrays? true})]

      (->> (rest result)
           (map (comp keyword first))
           set))))


(defn- -pull
  [{:as scope :keys [en am sg]}
   attrs
   mapping]

  ;; check e

  (let [qb (qb/builder)
        qp (qp/params)
        alias-sub :subquery
        qb-sub (qb/builder)
        qb-sub* (qb/builder)]

    (qb/add-select qb-sub :e)
    (qb/add-from qb-sub :datoms4)
    (qb-filter* qb-sub mapping)

    (qb/add-select qb-sub* :*)
    (qb/add-from qb-sub* :datoms4)
    (qb/add-where qb-sub* [:in :e (qb/->map qb-sub)])

    (qb/add-from qb [(qb/->map qb-sub*) alias-sub])
    (qb/add-select qb :e)
    (qb/add-select qb [:e "db/id"])
    (qb/add-group-by qb :e)

    (doseq [attr attrs]

      (let [multiple?  (am/multiple? am attr)
            attr-param (sql/param attr)
            pg-type    (am/get-pg-type am attr)
            agg        (if multiple? :array_agg :max)
            cast       (partial sql/call :cast)

            clause
            (sql/raw
             [(sql/call agg (cast :v (sql/inline pg-type)))
              " filter "
              {:where [:and [:= :a (sql/param attr)]]}])]

        (qp/add-param qp attr attr)
        (qb/add-select qb [clause (kw->str attr)])))

    (let [params (qp/get-params qp)]
      (en/query en (qb/format qb params)))))


(defn- pull-join [p1 p2 attr multiple?]

  (let [grouped (group-by :db/id p2)
        getter (fn [e]
                 (first (get grouped e)))
        updater (if multiple?
                  (fn [es]
                    (map getter es))
                  getter)]

    (for [p p1]
      (update p attr updater))))


(defn- find-attrs
  [pattern]
  (seq
   (reduce
    (fn [result node]
      (let [[tag node] node]
        (case tag
          :attr (conj result node)
          :map-spec (into result (keys node))
          result)))
    []
    pattern)))


(defn- find-links
  [pattern]
  (not-empty
   (reduce
    (fn [result [tag node]]
      (case tag
        :attr
        (if (am/-backref? node)
          (assoc result node '[*])
          result)
        :map-spec
        (merge result node)
        result))
    {}
    pattern)))


;; help with guessing attributes
;; limits


(defn pull-join-backref
  [p1 p2 attr]

  (let [attr-normal (am/backref->ref attr)
        grouped (group-by #(-> % attr-normal :db/id) p2)]
    (for [p p1]
      (assoc p attr (get grouped (:db/id p))))))

(defn process-backref
  [{:as scope :keys [am]}
   p attr pattern]

  (let [[_ pattern] pattern
        attr-normal (am/backref->ref attr)
        es (map :db/id p)
        mapping {:a (kw->str attr-normal)
                 (->cast :v :integer) es}
        p2 (pull-parsed scope pattern mapping attr-normal)]
    (pull-join-backref p p2 attr)))


(defn process-ref
  [{:as scope :keys [am]}
   p attr pattern]
  (let [[_ pattern] pattern
        multiple? (am/multiple? am attr)
        es (seq (if multiple?
                  (mapcat attr p)
                  (map attr p)))]
    (if es
      (let [mapping {:e es}
            p2 (pull-parsed scope pattern mapping)]
        (pull-join p p2 attr multiple?))
      p)))


(defn- pull-parsed
  [{:as scope :keys [am]}
   pattern
   mapping
   & attr-extra]

  (let [wc? (some (fn [x]
                    (some-> x first (= :wildcard)))
                  pattern)

        attrs-raw (find-attrs pattern)

        attrs (when-not wc?
                (->> attrs-raw
                     (filter (complement am/-backref?))))

        attrs-final
        (cond
          wc? (resolve-attrs scope mapping)
          attrs attrs
          :else
          (error! "No attributes in the pattern: %s" pattern))

        attrs-final (set (concat attrs-final attr-extra))

        links (find-links pattern)

        p1 (-pull scope attrs-final mapping)]

    (reduce
     (fn [p [attr pattern]]
       (if (am/-backref? attr)
         (process-backref scope p attr pattern)
         (process-ref scope p attr pattern)))
     p1
     links)))


(defn ->cast
  [field type]
  (sql/call :cast field (sql/inline type)))


(defn pull
  [scope
   pattern e]
  (let [mapping {:e e}

        ;; mapping {:a (kw->str :release/artist)
        ;;          (->cast :v :integer) [1 2 3 4 5]}

        parsed (s/conform ::ds/pattern pattern)]
    (if (= parsed ::s/invalid)
      (error! "Wrong pull pattern: %s" pattern)
      (pull-parsed scope parsed mapping))))


#_
(do

  (def _attrs
    [{:db/ident       :artist/name
      :db/valueType   :db.type/string
      :db/cardinality :db.cardinality/one}

     {:db/ident       :release/artist
      :db/valueType   :db.type/ref
      :db/cardinality :db.cardinality/one}

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
