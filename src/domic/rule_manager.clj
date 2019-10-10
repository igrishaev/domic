(ns domic.rule-manager
  (:require
   [domic.error :as e]))


(defprotocol IRuleManager

  (set-rules [this rules-map])

  (get-rule [this rule-name]))


(defrecord RuleManager
    [rules-map*]

  IRuleManager

  (set-rules [this rules-map]
    (reset! rules-map* rules-map))

  (get-rule [this rule-name]
    (or (get @rules-map* rule-name)
        (e/error! "No such a rule: %s" rule-name))))


(defn manager
  []
  (->RuleManager (atom {})))
