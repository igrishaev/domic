(ns domic.var-manager
  (:refer-clojure :exclude [bound?])
  (:require [domic.error :refer [error!]]))


(defprotocol IVarManager

  (gen-prefix [this])

  (getter [this var field])

  (get-form [this var])

  (bind! [this var val src form type])

  (bind [this var val src form type])

  (bound? [this var])

  (get-val [this var])

  (get-val! [this var]))


(defrecord VarManager
    [vars]

  IVarManager

  (getter [this var field]
    (get-in @vars [var field]))

  (get-form [this var]
    (getter this var :form))

  (get-val [this var]
    (getter this var :val))

  (get-val! [this var]
    (or (get-val this var)
        (error! "Var %s is unbound" var)))

  (gen-prefix [this]
    (str (gensym "d")))

  (bind! [this var val src form type]
    (if (bound? this var)
      (error! "Var %s is already bound: %s"
              var (get-form this var))
      (bind this var val src form type)))

  (bind [this var val src form type]
    (swap! vars assoc var
           {:val val
            :src src
            :form form
            :type type}))

  (bound? [this var]
    (contains? @vars var)))


(defn manager
  []
  (->VarManager (atom {})))


(def manager?
  (partial satisfies? IVarManager))
