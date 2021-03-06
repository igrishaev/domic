(ns domic.var-manager
  (:refer-clojure :exclude [bound?])
  (:require
   [domic.error :as e]))

;; todo
;; extend print


(def ^:dynamic *read-only* nil)


(defmacro with-read-only
  [& body]
  `(binding [*read-only* true]
     ~@body))


(defprotocol IVarManager

  (bind [this var val])

  (bound? [this var])

  (get-val [this var]))


(defrecord VarManager
    [vars]

  clojure.lang.IDeref

  (deref [this] @vars)

  IVarManager

  (get-val [this var]
    (if (bound? this var)
      (get @vars var)
      (e/error! "Var %s is unbound" var)))

  (bind [this var val]

    (when *read-only*
      (e/error! "Binding %s to %s is not allowed here"
              var val))

    (if (bound? this var)
      (e/error! "Var %s is already bound" var)
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


(defn subset
  [this valiables]
  (manager (into {} (for [var valiables]
                      [var (get-val this var)]))))
