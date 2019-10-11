(ns domic.query-formatter
  (:require
   [domic.error :as e]))

;; todo
;; make reflection clearer
;; type hints

(defonce hb?
  (try
    (import 'org.hibernate.engine.jdbc.internal.BasicFormatterImpl)
    true
    (catch Exception e
      false)))


(def hb-package
  '[org.hibernate/hibernate-core "5.4.6.Final"
    :exclusions
    [antlr/antlr
     com.fasterxml.jackson.core/jackson-core
     javax.activation/javax.activation-api
     javax.persistence/javax.persistence-api
     javax.xml.bind/jaxb-api
     net.bytebuddy/byte-buddy
     org.dom4j/dom4j
     org.glassfish.jaxb/jaxb-runtime
     org.hibernate.common/hibernate-commons-annotations
     org.javassist/javassist
     org.jboss/jandex
     org.jboss.logging/jboss-logging
     org.jboss.spec.javax.transaction/jboss-transaction-api_1.2_spec]])


(defn format-query
  [^String query]

  (if hb?

    (-> (resolve 'BasicFormatterImpl)
        (clojure.lang.Reflector/invokeConstructor (make-array Object 0))
        (.format ^String query))

    (e/error!
     (str "Warning: to format SQL queries, you've got to "
          "install Hibernate core package. Add this to your "
          "dev dependencies and restart the REPL: "
          \newline
          \newline
          (with-out-str
            (clojure.pprint/pprint hb-package))))))
