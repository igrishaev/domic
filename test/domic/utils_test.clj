(ns domic.utils-test
  (:require
   [clojure.test :refer [is]]))


(defmacro with-thrown?
  [re & body]
  `(~'is (~'thrown-with-msg?
          Exception ~re ~@body)))
