(ns domic.query-builder
  (:refer-clojure :exclude [format])
  (:require
   [clojure.pprint :refer [pprint]]
   [honeysql.helpers :as h]
   [honeysql.core :as sql]))


(defprotocol IQueryBuilder

  (add-join [this clause])

  (add-left-join [this table where])

  (last-column-index [this])

  (debug [this] [this params])

  (set-limit [this limit])

  (set-distinct [this])

  (add-select [this clause])

  (add-with [this clause])

  (add-group-by [this clause])

  (add-from [this clause])

  (add-where [this clause])

  (->map [this])

  (format [this] [this params]))


(def conj* (fnil conj []))


(defrecord QueryBuilder
    [sql]

  IQueryBuilder

  (add-join [this clause]
    (swap! sql h/merge-join clause))

  (add-left-join [this table where]
    (swap! sql h/merge-left-join table where))

  (last-column-index [this]
    (-> @sql :select count dec))

  (set-limit [this limit]
    (when limit
      (swap! sql h/limit limit)))

  (debug [this]
    (debug this {}))

  (debug [this params]
    (pprint (->map this))
    (println (format this params)))

  (set-distinct [this]
    (swap! sql h/merge-modifiers :distinct))

  (add-where [this clause]
    (swap! sql h/merge-where clause))

  (add-with [this clause]
    (swap! sql update :with conj* clause))

  (add-select [this clause]
    (swap! sql h/merge-select clause))

  (add-group-by [this clause]
    (swap! sql h/merge-group-by clause))

  (add-from [this clause]
    (swap! sql h/merge-from clause))

  (->map [this] @sql)

  (format [this]
    (format this nil))

  (format [this params]
    (sql/format (->map this)
                :params params
                :allow-namespaced-names? true
                :quoting :ansi)))


(defn builder
  []
  (->QueryBuilder (atom {})))


(def builder? (partial satisfies? IQueryBuilder))


#_
(do

  (doto (builder)
    (set-distinct)
    (add-where [:= :a :b])
    (add-where [:in :c [1 2 3]])
    (add-with [:sub :subquery])
    (add-select [:foo :f])
    (add-select [:bar :b])
    (add-group-by [:test :asc])
    (add-from [:datoms :d1])
    (add-from [:datoms :d2]))



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
