(ns domic.db-manager
  (:require
   [domic.error :as e]))


(defprotocol IDBManager

  (add-db [this var db])

  (get-db [this var])

  (get-db! [this var])

  (default-db [this])

  (default-db! [this]))


(defrecord DBManager
    [dbs]

  IDBManager

  (add-db [this var db]
    (swap! dbs assoc var db))

  (get-db [this var]
    (get @dbs var))

  (get-db! [this var]
    (or (get-db this var)
        (e/error! "No such db: %s" var)))

  (default-db [this]
    (some-> @dbs first second))

  (default-db! [this]
    (or (default-db this)
        (e/error! "No default database"))))


(defn manager
  []
  (->DBManager (atom (sorted-map))))
