(ns domic.var-manager
  (:refer-clojure :exclude [bound?])
  (:require [domic.error :refer [error!]]))


(defprotocol IVarManager

  (bind! [this var val])

  (bind [this var val])

  (bound? [this var])

  (get-val [this var])

  (get-val! [this var]))


(defrecord VarManager
    [vars]

  IVarManager

  (get-val [this var]
    (get @vars var))

  (get-val! [this var]
    (or (get-val this var)
        (error! "Var %s is unbound" var)))

  (bind! [this var val]
    (if (bound? this var)
      (error! "Var %s is already bound" var)
      (bind this var val)))

  (bind [this var val]
    (swap! vars assoc var val))

  (bound? [this var]
    (contains? @vars var)))


(defn manager
  []
  (->VarManager (atom {})))


(def manager?
  (partial satisfies? IVarManager))
