(ns domic.db-manager
  (:require
   [domic.error :refer [error!]]))


(defprotocol IDBManager

  (add-db [this var db])

  (default-db [this])

  (default-db! [this]))


(defrecord DBManager
    [dbs]

  IDBManager

  (add-db [this var db]
    (swap! dbs assoc var db))

  (default-db [this]
    (some-> @dbs first second))

  (default-db! [this]
    (or (default-db this)
        (error! "No default database"))))


(defn manager
  []
  (->DBManager (atom (sorted-map))))


(def manager? (partial satisfies? IDBManager))
