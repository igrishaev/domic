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

(defmacro with-where-and [qb & body]
  `(with-where ~qb :and ~@body))

(defmacro with-where-not-and [qb & body]
  `(with-where-not ~qb
     (with-where-and ~qb
       ~@body)))

(defmacro with-where-or-and [qb & body]
  `(with-where-or ~qb
     (with-where-and ~qb
       ~@body)))


(defn builder
  []
  (let [where-index (atom 0)
        where-stack (atom [(atom [:and])])

        sql (atom {:select []
                   :from []})]

    (reify IQueryBuilder

      (where-stack-up [this op]
        (swap! where-index inc)
        (swap! where-stack conj (atom [op])))

      (where-stack-down [this]
        (swap! where-index dec))

      (add-select [this select]
        (swap! sql update :select conj select))

      (add-from [this from]
        (swap! sql update :from conj from))

      (add-where [this where]
        (let [where-node (get @where-stack @where-index)]
          (swap! where-node conj where)))

      (->map [this]
        (let [stacks (reverse (map deref @where-stack))
              where (reduce (fn [where stack]
                              (conj stack where))
                            (first stacks)
                            (rest stacks))]
          (assoc @sql :where where)))

      (format [this]
        (sql/format (->map this))))))
