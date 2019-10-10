(ns domic.attr-manager
  (:require
   [clojure.set :as set]

   [domic.attributes :as at]
   [domic.util :as u]
   [domic.error :as e]))


(defn- group-attrs
  [attr-list]
  (reduce
   (fn [result attrs]
     (let [{:db/keys [ident]} attrs]
       (assoc result ident attrs)))
   {}
   attr-list))


(defprotocol IAttrManager

  (-get-field [this attr field])

  (-has-field? [this attr field])

  (-field-equal? [this attr field value])

  (validate-many! [this attrs])

  (reset [this attr-list])

  (known? [this attr])

  (index? [this attr])

  (component? [this attr])

  (by-wildcard [this attr])

  (ref? [this attr])

  (unique? [this attr])

  (unique-value? [this attr])

  (unique-identity? [this attr])

  (multiple? [this attr])

  (get-type [this attr])

  (db-type [this attr]))


(defrecord AttrManager
    [attr-map*]

  clojure.lang.IDeref

  (deref [this] (set (keys @attr-map*)))

  IAttrManager

  (-get-field [this attr field]
    (get-in @attr-map* [attr field]))

  (-has-field? [this attr field]
    (some? (-get-field this attr field)))

  (-field-equal? [this attr field value]
    (= (-get-field this attr field) value))

  (validate-many! [this attrs]
    (when-let [diff (not-empty
                     (set/difference
                      (set attrs)
                      @this))]
      (e/error! "Unknown attr(s): %s" (u/join diff))))

  (reset [this attr-list]
    (let [attrs* (-> at/defaults
                     (concat attr-list)
                     group-attrs)]
      (reset! attr-map* attrs*)))

  (known? [this attr]
    (contains? @attr-map* attr))

  (index? [this attr]
    (-has-field? this attr :db/index))

  (unique-value? [this attr]
    (-field-equal? this attr :db/unique :db.unique/value))

  (unique-identity? [this attr]
    (-field-equal? this attr :db/unique :db.unique/identity))

  (unique? [this attr]
    (-has-field? this attr :db/unique))

  (component? [this attr]
    (-field-equal? this attr :db/isComponent true))

  (multiple? [this attr]
    (-field-equal? this attr :db/cardinality :db.cardinality/many))

  (ref? [this attr]
    (-field-equal? this attr :db/valueType :db.type/ref))

  (by-wildcard [this attr]
    (let [a-ns (namespace attr)
          a-keys @this]
      (set
       (for [a-key a-keys
             :when (= (namespace a-key) a-ns)]
         a-key))))

  (get-type [this attr]
    (or
     (-get-field this attr :db/valueType)
     (e/error! "Unknown attrubute %s" attr)))

  (db-type [this attr]
    (let [a-type (get-type this attr)]
      (at/->db-type a-type))))


(defn manager
  ([]
   (manager nil))
  ([attr-list]
   (doto (->AttrManager (atom {}))
     (reset attr-list))))


(u/extend-print AttrManager)
