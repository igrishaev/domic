(ns domic.spec
  (:require
   [clojure.spec.alpha :as s]))


(s/def ::?var
  (fn [value]
    (and (symbol? value)
         (some-> value name first (= \?)))))


(s/def :find/entry
  (s/cat :var ::?var))

(s/def :q/find
  (s/coll-of :find/entry))


(s/def :find/section
  (s/cat :tag (partial = :find)
         :find (s/+ :find/entry)))


(s/def ::attr-or-bind
  (s/alt :var ::?var
         :attr keyword?))


(s/def ::scalar
  (some-fn string? number? boolean?))

(s/def ::value-or-bind
  (s/alt :var ::?var
         :value ::scalar))

(s/def :where/split
  (s/cat :e ::?var
         :a ::attr-or-bind
         :v ::value-or-bind))


(s/def :where/entry
  (s/and vector? :where/split))


(s/def :where/section
  (s/cat :tag (partial = :where)
         :where (s/+ :where/entry)))

(s/def ::q
  (s/cat :find :find/section
         :where :where/section

         )


  #_

  (s/tuple #(= % :find) (s/cat :find :find/entry)))


(def q
  '
  [:find ?e ?year
   :where
   [?e :artist/name "Queen"]
   [?r :release/artist ?e]
   ;; [?r :release/year ?year]
   ;; (> ?year 1970)

   ]

  )

#_
(s/conform ::q q)



(s/def :q/section
  (s/cat :tag keyword?
         :form (s/+ (complement keyword?))))

(s/def ::q2
  (s/+ :q/section))


#_
(reduce
 (fn [result {:keys [tag form]}]
   (assoc result tag form))
 {}
 (s/conform ::q2 q))


(s/def :q/where
  (s/coll-of
   (s/alt :bind :where/split)))

(s/def ::q3
  (s/keys :req-un [
                   :q/where]))
