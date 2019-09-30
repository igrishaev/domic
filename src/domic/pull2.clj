(ns domic.pull2
  (:require
   [domic.query-builder :as qb]
   [domic.query-params :as qp]
   [domic.attr-manager :as am]
   [domic.engine :as en])
  (:import java.sql.ResultSet))


#_
(def conj-set (fnil conj #{}))

#_
(defn rs->maps
  [{:as scope :keys [am]}]
  (fn [^ResultSet rs]
    (loop [next? (.next rs)
           result {}]
      (if next?
        (let [{:as row :keys [e a v]} (rs->datom scope rs)
              m? (am/multiple? am a)]
          (recur (.next rs)
                 (-> (if m?
                       (update-in result [e a] conj-set v)
                       (assoc-in result [e a] v))
                     (assoc-in [e :db/id] e))))
        result))))


(defn- rs->datom
  [{:as scope :keys [am]}
   ^ResultSet rs]
  (let [attr (keyword (.getString rs 3))
        attr-type (am/get-db-type am attr)]
    {:id (.getLong rs 1)
     :e  (.getLong rs 2)
     :a  attr
     :v  (am/rs->clj attr-type rs 4)
     :t  (.getLong rs 5)}))


(defn- rs->datoms
  [scope]
  (fn [^ResultSet rs]
    (let [result* (transient [])]
      (while (.next rs)
        (conj! result* (rs->datom scope rs)))
      (persistent! result*))))


(defn pull*
  [{:as scope :keys [en]}
   & [elist alist]]

  (let [qb (qb/builder)
        qp (qp/params)
        add-alias (partial qp/add-alias qp)]

    (qb/add-select qb :*)
    (qb/add-from qb :datoms4)

    (when elist
      (qb/add-where qb [:in :e (mapv add-alias elist)]))

    (when alist
      (qb/add-where qb [:in :a (mapv add-alias alist)]))

    (en/query-rs en
                 (->> (qp/get-params qp)
                      (qb/format qb))
                 (rs->datoms scope))))
