(ns domic.attributes
  (:require
   [clojure.string :as str]
   [domic.error :as e])

  (:import
   clojure.lang.Keyword
   java.net.URI
   java.util.UUID
   java.sql.ResultSet))

;; todo
;; bytes -> base64

;; :db.type/keyword - Value type for keywords. Keywords are used as names, and are interned for efficiency. Keywords map to the native interned-name type in languages that support them.
;; :db.type/symbol  - Value type for symbols. Symbols map to the symbol type in languages that support them, e.g. clojure.lang.Symbol in Clojure
;; :db.type/string  - Value type for strings.
;; :db.type/boolean - Boolean value type.
;; :db.type/long    - Fixed integer value type. Same semantics as a Java long: 64 bits wide, two's complement binary representation.
;; :db.type/bigint  - Value type for arbitrary precision integers. Maps to java.math.BigInteger on Java platforms.
;; :db.type/float   - Floating point value type. Same semantics as a Java float: single-precision 32-bit IEEE 754 floating point.
;; :db.type/double  - Floating point value type. Same semantics as a Java double: double-precision 64-bit IEEE 754 floating point.
;; :db.type/bigdec  - Value type for arbitrary precision floating point numbers. Maps to java.math.BigDecimal on Java platforms.
;; :db.type/ref     - Value type for references. All references from one entity to another are through attributes with this value type.
;; :db.type/instant - Value type for instants in time. Stored internally as a number of milliseconds since midnight, January 1, 1970 UTC. Maps to java.util.Date on Java platforms.
;; :db.type/uuid    - Value type for UUIDs. Maps to java.util.UUID on Java platforms.
;; :db.type/uri     - Value type for URIs. Maps to java.net.URI on Java platforms.
;; :db.type/tuple   - Value type for a tuple of scalar values.
;; :db.type/bytes   - Value type for small binary data. Maps to byte array on Java platforms. See limitations.

;; :db/valueType
;; :db/cardinality
;; :db/doc
;; :db/unique
;; :db.attr/preds
;; :db/index
;; :db/fulltext
;; :db/isComponent
;; :db/noHistory


(def defaults

  [{:db/ident       :db/valueType
    :db/valueType   :db.type/keyword
    :db/index       true
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/cardinality
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/doc
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/unique
    :db/valueType   :db.type/keyword
    :db/unique      :db.unique/value
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/index
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/fulltext
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/isComponent
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/noHistory
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/ident
    :db/valueType   :db.type/keyword
    :db/unique      :db.unique/identity
    :db/cardinality :db.cardinality/one}

   {:db/ident       :db/txInstant
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}])


(defn rs->clj
  [^Keyword valueType
   ^ResultSet rs
   ^long index]

  (case valueType

    :db.type/keyword
    (-> (.getString rs index) keyword)

    :db.type/symbol
    (-> (.getString rs index) symbol)

    :db.type/string
    (.getString rs index)

    :db.type/boolean
    (.getBoolean rs index)

    :db.type/long
    (.getLong rs index)

    ;; java.math.BigInteger
    :db.type/bigint
    (e/error! "BigInt is not implemented")

    :db.type/float
    (.getFloat rs index)

    :db.type/double
    (.getDouble rs index)

    :db.type/bigdec
    (.getBigDecimal rs index)

    :db.type/ref
    (as-> (.getLong rs index) id {:db/id id})

    :db.type/instant
    (.getDate rs index)

    :db.type/uuid
    (-> (.getString rs index) UUID/fromString)

    :db.type/uri
    (as-> (.getString rs index) uri (new URI uri))

    :db.type/tuple
    (e/error! "Tuples are not implemented")

    :db.type/bytes
    (.getBytes rs index)

    ;; else
    (e/error-case! valueType)))


(defn ->db-type
  [valueType]

  (case valueType
    :db.type/keyword :text
    :db.type/symbol  :text
    :db.type/string  :text
    :db.type/boolean :boolean
    :db.type/long    :bigint
    :db.type/bigint  :decimal
    :db.type/float   :real
    :db.type/double  (keyword "double precision")
    :db.type/bigdec  :decimal
    :db.type/ref     :bigint
    :db.type/instant (keyword "timestamp with timezone")
    :db.type/uuid    :uuid
    :db.type/uri     :text
    :db.type/tuple   (e/error! "Tuples are not implemented")
    :db.type/bytes   (e/error! "Bytes are not implemented")

    ;; else
    (e/error-case! valueType)))


(defn attr-wildcard?
  [^Keyword attr]
  (some-> attr name (= "*")))


(defn attr-backref?
  [^Keyword attr]
  (some-> attr name (str/starts-with? "_")))


(defn attr-backref->ref
  [^Keyword attr]
  (let [a-ns (namespace attr)
        a-name (name attr)]
    (keyword a-ns (subs a-name 1))))
