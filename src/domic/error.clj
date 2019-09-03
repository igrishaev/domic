(ns domic.error)


(defn error!
  [message & args]
  (throw (new Exception ^String (apply format message args))))
