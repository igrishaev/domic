(ns domic.var-manager
  (:refer-clojure :exclude [bound?])
  (:require [domic.error :refer [error!]]))


(def ^:dynamic *read-only* nil)


(defmacro with-read-only
  [& body]
  `(binding [*read-only* true]
     ~@body))


(defprotocol IVarManager

  (subset [this valiables])

  (consume [this other])

  (bind [this var val])

  (bound? [this var])

  (get-val [this var]))


(defrecord VarManager
    [vars]

  clojure.lang.IDeref

  (deref [this] @vars)

  IVarManager

  (subset [this valiables]
    (manager (into {} (for [var valiables]
                        [var (get-val this var)]))))

  (consume [this other]
    (swap! vars merge @other))

  (get-val [this var]
    (if (bound? this var)
      (get @vars var)
      (error! "Var %s is unbound" var)))

  (bind [this var val]

    (when *read-only*
      (error! "Binding %s to %s is not allowed here"
              var val))

    (if (bound? this var)
      (error! "Var %s is already bound" var)
      (swap! vars assoc var val)))

  (bound? [this var]
    (contains? @vars var)))


(defn manager
  ([]
   (manager {}))

  ([initials]
   (->VarManager (atom initials))))


(def manager?
  (partial satisfies? IVarManager))
