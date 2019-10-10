(ns domic.source-manager
  (:require
   [domic.error :as e]))


(defprotocol ISourceManager

  (has-source? [this name])

  (add-source [this name source])

  (get-source [this name]))


(defrecord SourceManager
    [-state*]

  ISourceManager

  (has-source? [this name]
    (contains? @-state* name))

  (add-source [this name source]
    (if (has-source? this name)
      (e/error! "Source %s has already been added" name)
      (swap! -state* assoc name source)))

  (get-source [this name]
    (or (get @-state* name)
        (e/error! "No such a source: %s" name))))


(defn manager
  []
  (->SourceManager (atom {})))
