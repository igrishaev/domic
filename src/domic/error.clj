(ns domic.error)


(defn error!
  [message & args]
  (throw (new Exception ^String (apply format message args))))


(defn error-case!
  [data]
  (error! "Missing case: %s" data))
