(ns domic.attr-manager)


(defprotocol IAttrManager

  (multiple? [this attr])

  (get-db-type [this attr])

  (get-pg-type [this attr]))


(defn manager
  [attributes]
  (let [grouped (reduce (fn [result attrs]
                          (let [{:db/keys [ident]} attrs]
                            (assoc result ident attrs)))
                        {}
                        attributes)

        attrs (atom grouped)]

    (reify IAttrManager

      (multiple? [this attr]
        (some-> @attrs
                (get attr)
                :db/cardinality
                (= :db.cardinality/many)))

      (get-db-type [this attr]
        (get-in @attrs [attr :db/valueType]))

      (get-pg-type [this attr]
        (case (get-db-type this attr)
          :db.type/string  :text
          :db.type/ref     :integer
          :db.type/integer :integer)))))
