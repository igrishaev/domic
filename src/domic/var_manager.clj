(ns domic.var-manager
  (:refer-clojure :exclude [bound?]))


(defprotocol IVarManager

  (gen-prefix [this])

  (bind [this var src val])

  (bound? [this var])

  (get-val [this var]))


(defn manager
  []
  (let [vars (atom {})]
    (reify IVarManager

      (gen-prefix [this]
        (str (gensym "d")))

      (bind [this var src val]
        (swap! vars assoc var {:src src
                               :val val}))

      (bound? [this var]
        (contains? @vars var))

      (get-val [this var]
        (get-in @vars [var :val])))))
