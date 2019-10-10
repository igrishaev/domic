(ns domic.spec-datomic
  (:require
   [clojure.spec.alpha :as s]
   [clojure.string :as str]))



;; ---- Common --------------------------------------------------------------

;; (s/def :datomic-spec.core.connect/protocol
;;   #{:sql :cass})

;; (s/def :datomic-spec.core.connect/db-name
;;   string?)

;; (s/def :datomic-spec.core.connect.sql/data-source
;;   some?)

;; (s/def :datomic-spec.core.connect.sql/factory
;;   some?)

;; (s/def :datomic-spec.core.connect.cass/table
;;   string?)

;; (s/def :datomic-spec.core.connect.cass/cluster
;;   some?)

;; (defmulti connect-map-form :protocol)

;; (defmethod connect-map-form :sql [_]
;;   (println :foo)
;;   (s/keys :req-un [:datomic-spec.core.connect/protocol
;;                    :datomic-spec.core.connect/db-name
;;                    (or :datomic-spec.core.connect.sql/data-source
;;                        :datomic-spec.core.connect.sql/factory)]))

;; (defmethod connect-map-form :cass [_]
;;   (s/keys :req-un [:datomic-spec.core.connect/protocol
;;                    :datomic-spec.core.connect/db-name
;;                    :datomic-spec.core.connect.cass/table
;;                    :datomic-spec.core.connect.cass/cluster]))

;; (s/def ::connect-map-form
;;   (s/multi-spec connect-map-form :protocol))

;; (s/def ::connect-uri
;;   (s/or :string-form (s/and string? #(try (URI. %) (catch URISyntaxException _)))
;;         :map-form ::connect-map-form))

;; (s/def ::conn
;;   #(instance? Connection %))

;; (s/def ::db
;;   #(instance? Database %))

;; (s/def ::log
;;   #(instance? Log %))

;; (s/def ::datom
;;   (s/spec #(instance? Datom %)
;;           :gen #(gen/fmap (fn [[e a v t]] (Datum. e a v t))
;;                           (gen/tuple (s/gen nat-int?)
;;                                      (s/gen nat-int?)
;;                                      (s/gen string?)
;;                                      (s/gen nat-int?)))))

;; (s/def ::entity
;;   #(instance? Entity %))

;; (s/def ::tempids
;;   (s/map-of (s/or :string string? :int int?) nat-int?))

;; (s/def ::tempid
;;   (s/or :string string? :id #(instance? DbId %)))

;; (s/def ::index-type
;;   #{:eavt :aevt :avet :vaet})

;; ---- TX Data ---------------------------------------------------------------

;; (s/def ::addition
;;   (s/cat :op #{:db/add}
;;          :eid ::entity-id
;;          :attr keyword?
;;          :val any?))

;; (s/def ::retraction
;;   (s/cat :op #{:db/retract}
;;          :eid ::entity-id
;;          :attr keyword?
;;          :val any?))

;; (s/def ::map-form map?)

;; (s/def ::transact-fn-call
;;   (s/cat :fn keyword? :args (s/* any?)))

;; (s/def ::tx-stmt
;;   (s/or :assertion ::addition
;;         :retraction ::retraction
;;         :map-form ::map-form
;;         :transact-fn-call ::transact-fn-call))

;; (s/def ::tx-data
;;   (s/coll-of ::tx-stmt))

;; ---- Pull Pattern ----------------------------------------------------------

(s/def ::pattern
  (s/spec (s/+ ::attr-spec)))

(s/def ::attr-spec
  (s/or :attr ::attr-name
        :wildcard ::wildcard
        :map-spec ::map-spec
        :attr-expr ::attr-expr))

(s/def ::attr-name
  keyword?)

(s/def ::wildcard
  #{"*" '*})

(s/def ::map-spec
  (s/map-of (s/or :attr ::attr-name
                  :limit-expr ::limit-expr)
            (s/or :pattern ::pattern
                  :recursion-limit ::recursion-limit)
            :min-count 1))

(s/def ::attr-expr
  (s/or :limit-expr ::limit-expr
        :default-expr ::default-expr))

(s/def ::limit-expr
  (s/cat :key #{"limit" 'limit}
         :attr ::attr-name
         :limit (s/alt :pos-int pos-int?
                       :nil nil?)))

(s/def ::default-expr
  (s/cat :key #{"default" 'default}
         :attr ::attr-name
         :val any?))


(s/def ::recursion-limit
  (s/or :pos-int pos-int? :ellipsis #{'...}))

;; ;; ---- Entity Identifier -----------------------------------------------------

;; (s/def ::entity-id nat-int?)

;; ; An entity identifier is any one of the three ways that Datomic can uniquely
;; ; identity an entity: an entity id, ident, or lookup ref. Most Datomic APIs
;; ; that refer to entities take entity identifiers as arguments.

;; (s/def ::entity-identifier
;;   (s/or :entity-id ::entity-id
;;         :ident ::attr-name
;;         :lookup-ref ::lookup-ref))

;; ;; ---- Lookup Ref ------------------------------------------------------------

;; ; A lookup ref is a list containing an attribute and a value. It identifies
;; ; the entity with the given unique attribute value.

;; (s/def ::lookup-ref
;;   (s/cat :attr-name ::attr-name :val any?))

;; ---- Query -----------------------------------------------------------------

(defmulti query-form (fn [query] (if (map? query) :map :list)))

(defmethod query-form :map [_]
  (s/keys :req-un [::find] :opt-un [::with ::in ::where]))

(defmethod query-form :list [_]
  (s/cat :find (s/cat :find-kw #{:find} :spec ::find-spec)
         :with (s/? (s/cat :with-kw #{:with} :vars (s/+ ::variable)))
         :in (s/? (s/cat :in-kw #{:in} :inputs (s/+ ::input)))
         :where (s/? (s/cat :where-kw #{:where} :clauses (s/+ ::clause)))))

(s/def ::query
  (s/multi-spec query-form (fn [g _] g)))

(s/def ::find
  (s/cat :spec ::find-spec))

(s/def ::with
  (s/cat :vars (s/+ ::variable)))

(s/def ::in
  (s/cat :inputs (s/+ ::input)))

(s/def ::where
  (s/cat :clauses (s/+ ::clause)))

(s/def ::find-spec
  (s/alt :rel ::find-rel
         :coll ::find-coll
         :tuple ::find-tuple
         :scalar ::find-scalar))

(s/def ::find-rel
  (s/+ ::find-elem))

(s/def ::find-coll
  (s/spec (s/cat :elem ::find-elem :ellipsis #{'...})))

(s/def ::find-scalar
  (s/cat :elem ::find-elem :period #{'.}))

(s/def ::find-tuple
  (s/+ ::find-elem))

(s/def ::find-elem
  (s/or :var ::variable
        :pull-expr ::pull-expr
        :agg ::aggregate))

(s/def ::pull-expr
  (s/spec (s/cat :op #{'pull}
                 :var ::variable
                 :pattern ::pattern)))

(s/def ::aggregate
  (s/spec (s/cat :name symbol? :args (s/+ ::fn-arg))))

(s/def ::fn-arg
  (s/or :var ::variable :cst ::constant :src-var ::src-var))

(s/def ::where-clauses
  (s/cat :op #{:where} :clauses (s/+ ::clause)))

(s/def ::input
  (s/or :src-var ::src-var
        :binding ::binding
        :rules-var ::rules-var
        :pattern-var ::pattern-var))

(s/def ::src-var
  (s/and simple-symbol? #(str/starts-with? (name %) "$")))

(s/def ::variable
  (s/and simple-symbol? #(str/starts-with? (name %) "?")))

(s/def ::rules-var
  #{'%})

(s/def ::plain-symbol
  (s/and simple-symbol?
         #(not (str/starts-with? (name %) "$"))
         #(not (str/starts-with? (name %) "?"))))

(s/def ::pattern-var
  ::plain-symbol)

(s/def ::rule-expr
  (s/cat :src-var (s/? ::src-var)
         :rule-name ::rule-name
         :vars (s/+ (s/alt :var ::variable
                           :cst ::constant
                           :unused #{'_}))))

(s/def ::bool-expr
  (s/cat :src-var (s/? ::src-var)
         :op #{'not 'or 'and}
         :clauses (s/+ (s/or :pred-expr ::pred-expr
                             :bool-expr ::bool-expr))))

(s/def ::rule-vars
  (s/or :vars (s/+ ::variable)
        :vars* (s/cat :in (s/spec (s/+ ::variable)) :out (s/* ::variable))))

(s/def ::clause
  (s/or :bool-expr    ::bool-expr
        :data-pattern ::data-pattern
        :pred-expr    ::pred-expr
        :fn-expr      ::fn-expr
        :rule-expr    ::rule-expr))

(s/def ::data-pattern
  (s/spec (s/cat :src-var (s/? ::src-var)
                 :elems (s/+ (s/alt :var ::variable
                                    :cst ::constant
                                    :blank #{'_})))))

(s/def ::constant
  (s/or :str string? :num number? :kw keyword? :bool boolean?))

(s/def ::pred-expr
  (s/spec (s/cat :expr (s/spec (s/cat :pred (s/alt :sym symbol?
                                                   :set set?)
                                      :args (s/+ ::fn-arg))))))

(s/def ::fn-expr
  (s/spec (s/cat :expr (s/spec (s/cat :fn (s/alt :sym symbol?
                                                 :set set?)
                                      :args (s/+ ::fn-arg)))
                 :binding ::binding)))

(s/def ::binding
  (s/or :bind-scalar ::bind-scalar
        :bind-tuple ::bind-tuple
        :bind-coll ::bind-coll
        :bind-rel ::bind-rel))

(s/def ::bind-scalar
  ::variable)

(s/def ::bind-tuple
  (s/spec (s/+ (s/alt :var ::variable :unused #{'_}))))

(s/def ::bind-coll
  (s/spec (s/cat :var ::variable :ellipsis #{'...})))

(s/def ::bind-rel
  (s/coll-of (s/spec (s/+ (s/alt :var ::variable :unused #{'_})))))

(s/def ::rules
  (s/spec (s/+ ::rule)))

(s/def ::rule
  (s/spec (s/cat :head ::rule-head
                 :clauses (s/+ ::clause))))

(s/def ::rule-name
  ::plain-symbol)

(s/def ::rule-head
  (s/spec (s/cat :name ::rule-name
                 :vars-req (s/? (s/spec (s/+ ::variable)))
                 :vars-opt (s/* ::variable))))


;; ---- Special ---------------------------------------------------------------

;; (s/def :datomic-spec.query/args
;;   (s/spec (s/+ any?)))

;; (s/def :datomic-spec.query/timeout
;;   nat-int?)

;; (s/def ::tx-num-tx-id-date
;;   (s/alt :tx-num nat-int? :tx-id nat-int? :date inst?))

;; ---- Schema ----------------------------------------------------------------

;; (s/def :db/id ::entity-id)

;; (s/def :db/unique #{:db.unique/value :db.unique/identity})

;; (s/def :db/valueType #{:db.type/keyword :db.type/string :db.type/boolean
;;                        :db.type/long :db.type/bigint :db.type/float
;;                        :db.type/double :db.type/bigdec :db.type/ref
;;                        :db.type/instant :db.type/uuid :db.type/uri
;;                        :db.type/bytes})

;; (s/def :db/cardinality #{:db.cardinality/one :db.cardinality/many})

;; ---- Spec Overrides --------------------------------------------------------

;; (def spec-overrides
;;   {`d/as-of
;;    (s/fspec :args (s/cat :db ::db :t ::tx-num-tx-id-date))

;;    `d/as-of-t
;;    (s/fspec :args (s/cat :db ::db))

;;    `d/attribute
;;    (s/fspec :args (s/cat :db ::db :attrid (s/alt :entity-id nat-int?
;;                                                  :ident ::attr-name)))
;;    `d/basis-t
;;    (s/fspec :args (s/cat :db ::db))

;;    `d/connect
;;    (s/fspec :args (s/cat :uri ::connect-uri))

;;    `d/create-database
;;    (s/fspec :args (s/cat :uri string?))

;;    `d/datoms
;;    (s/fspec :args (s/cat :db ::db
;;                          :index ::index-type
;;                          :components (s/* any?)))

;;    `d/db
;;    (s/fspec :args (s/cat :conn any?))

;;    `d/delete-database
;;    (s/fspec :args (s/cat :uri string?))

;;    `d/entid
;;    (s/fspec :args (s/cat :db ::db :ident ::entity-identifier))

;;    `d/entid-at
;;    (s/fspec :args (s/cat :db ::db
;;                          :part ::entity-identifier
;;                          :t-or-date (s/alt :t nat-int? :date inst?)))

;;    `d/entity
;;    (s/fspec :args (s/cat :db ::db :eid ::entity-identifier))

;;    `d/entity-db
;;    (s/fspec :args (s/cat :entity ::entity))

;;    `d/filter
;;    (s/fspec :args (s/cat :db ::db :pred fn?))

;;    `d/get-database-names
;;    (s/fspec :args (s/cat :uri string?))

;;    `d/history
;;    (s/fspec :args (s/cat :db ::db))

;;    `d/ident
;;    (s/fspec :args (s/cat :db ::db :eid (s/alt :entity-id nat-int?
;;                                               :attr ::attr-name)))

;;    `d/index-range
;;    (s/fspec :args (s/cat :db ::db :attrid ::entity-identifier
;;                          :start (s/nilable nat-int?) :end (s/nilable nat-int?)))

;;    `d/is-filtered
;;    (s/fspec :args (s/cat :db ::db))

;;    `d/log
;;    (s/fspec :args (s/cat :conn ::conn) :ret (s/nilable ::log))

;;    `d/next-t
;;    (s/fspec :args (s/cat :db ::db))

;;    `d/part
;;    (s/fspec :args (s/cat :eid nat-int?))

;;    `d/pull
;;    (s/fspec :args (s/cat :db ::db
;;                          :pattern (s/spec ::pattern)
;;                          :eid ::entity-identifier))
;;    `d/pull-many
;;    (s/fspec :args (s/cat :db ::db
;;                          :pattern (s/spec ::pattern)
;;                          :eid (s/every ::entity-identifier)))

;;    `d/q
;;    (s/fspec :args (s/cat :query ::query :inputs (s/+ any?)))

;;    `d/query
;;    (s/fspec :args (s/cat :query-map (s/keys :req-un [::query :datomic-spec.query/args]
;;                                             :opt-un [:datomic-spec.query/timeout])))

;;    `d/release
;;    (s/fspec :args (s/cat :conn ::conn))

;;    `d/remove-tx-report-queue
;;    (s/fspec :args (s/cat :conn ::conn))

;;    `d/rename-database
;;    (s/fspec :args (s/cat :uri string? :new-name string?))

;;    `d/request-index
;;    (s/fspec :args (s/cat :conn ::conn))

;;    `d/resolve-tempid
;;    (s/fspec :args (s/cat :db ::db :tempids ::tempids :tempid ::tempid))

;;    `d/seek-datoms
;;    (s/fspec :args (s/cat :db ::db
;;                          :index ::index-type
;;                          :components (s/* ::entity-identifier)))

;;    `d/shutdown
;;    (s/fspec :args (s/cat :shutdown-clojure boolean?))

;;    `d/since
;;    (s/fspec :args (s/cat :db ::db :t ::tx-num-tx-id-date))

;;    `d/since-t
;;    (s/fspec :args (s/cat :db ::db))

;;    `d/squuid
;;    (s/fspec :args (s/cat) :ret uuid?)

;;    `d/squuid-time-millis
;;    (s/fspec :args (s/cat :squuid uuid?) :ret nat-int?)

;;    `d/sync
;;    (s/fspec :args (s/cat :conn ::conn :t (s/? nat-int?)))

;;    `d/sync-excise
;;    (s/fspec :args (s/cat :conn ::conn :t nat-int?))

;;    `d/sync-index
;;    (s/fspec :args (s/cat :conn ::conn :t nat-int?))

;;    `d/sync-schema
;;    (s/fspec :args (s/cat :conn ::conn :t nat-int?))

;;    ;; TODO: errors in clojure.spec.test.alpha$spec_checking_fn$fn__3026 cannot be cast to clojure.lang.IFn$LO
;;    ;; `d/t->tx
;;    ;; (s/fspec :args (s/cat :t nat-int?) :ret nat-int?)

;;    `d/tempid
;;    (s/fspec :args (s/cat :partition keyword? :n (s/? int?)) :ret ::tempid)

;;    `d/touch
;;    (s/fspec :args (s/cat :entity ::entity))

;;    `d/transact
;;    (s/fspec :args (s/cat :connection ::conn :tx-data ::tx-data))

;;    `d/transact-async
;;    (s/fspec :args (s/cat :connection ::conn :tx-data ::tx-data))

;;    ;; TODO: error in clojure.spec.test.alpha$spec_checking_fn$fn__3026 cannot be cast to clojure.lang.IFn$OL
;;    ;; `d/tx->t
;;    ;; (s/fspec :args (s/cat :tx nat-int?) :ret nat-int?)

;;    `d/tx-range
;;    (s/fspec :args (s/cat :log ::log
;;                          :start (s/nilable ::tx-num-tx-id-date)
;;                          :end (s/nilable ::tx-num-tx-id-date)))

;;    `d/tx-report-queue
;;    (s/fspec :args (s/cat :connection ::conn))

;;    `d/with
;;    (s/fspec :args (s/cat :db ::db :tx-data ::tx-data))})
