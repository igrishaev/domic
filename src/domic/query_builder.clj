(ns domic.query-builder
  (:refer-clojure :exclude [format])
  (:require [honeysql.core :as sql]))


(defprotocol IQueryBuilder

  (where-stack-up [this op])

  (where-stack-down [this])

  (add-select [this select])

  (add-from [this from])

  (add-where [this where])

  (->map [this])

  (format [this]))


(defmacro with-where [qb op & body]
  `(do
     (where-stack-up ~qb ~op)
     (try
       ~@body
       (finally
         (where-stack-down ~qb)))))

(defmacro with-where-not [qb & body]
  `(with-where ~qb :not ~@body))

(defmacro with-where-or [qb & body]
  `(with-where ~qb :or ~@body))

(defmacro with-where-not-and [qb & body]
  `(with-where-not ~qb
     (with-where-and ~qb
       ~@body)))

(defmacro with-where-not-or [qb & body]
  `(with-where-not ~qb
     (with-where-or ~qb
       ~@body)))


(defn update-in*
  [data path func & args]
  (if (empty? path)
    (apply func data args)
    (apply update-in data path func args)))


(defrecord QueryBuilder
    [where
     where-path
     sql]

  IQueryBuilder

  (where-stack-up [this op]
    (let [index (count (get-in @where @where-path))]
      (swap! where update-in* @where-path conj [op])
      (swap! where-path conj index)))

  (where-stack-down [this]
    (swap! where-path (comp vec butlast)))

  (add-select [this select]
    (swap! sql update :select conj select))

  (add-from [this from]
    (swap! sql update :from conj from))

  (add-where [this clause]
    (swap! where update-in* @where-path conj clause))

  (->map [this]
    (assoc @sql :where @where))

  (format [this]
    (sql/format (->map this))))


(defn builder
  []
  (->QueryBuilder (atom [:and])
                  (atom [])
                  (atom {:select [] :from []})))


#_
(do

  (def _b (builder))

  (add-where _b [:= 1 1])
  (add-where _b [:= 2 2])
  (with-where-not _b
    (add-where _b [:= 3 3]))
  (with-where-not-and _b
    (add-where _b [:= 4 4])
    (add-where _b [:= 5 5]))
  (with-where-or _b
    (add-where _b [:= 6 6])
    (add-where _b [:= 7 7]))

  (clojure.pprint/pprint (->map _b)))