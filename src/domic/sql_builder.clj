(ns domic.sql-builder
  (:refer-clojure :exclude [format map])
  (:require [honeysql.core :as sql]))


(defprotocol ISQLBuilder

  (add-select [this select])

  (add-from [this from])

  (add-where [this where])

  (map [this])

  (format [this]))


(defn builder
  []
  (let [sql (atom {:select []
                   :from []
                   :where [:and]})]

    (reify ISQLBuilder

      (add-select [this select]
        (swap! sql update :select conj select))

      (add-from [this from]
        (swap! sql update :from conj from))

      (add-where [this where]
        (swap! sql update :where conj where))

      (map [this]
        @sql)

      (format [this]
        (sql/format @sql)))))
