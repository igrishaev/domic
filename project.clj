(defproject domic "0.1.0-SNAPSHOT"

  :description "Datomic on top of SQL"

  :url "https://github.com/igrishaev/domic"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [[org.clojure/java.jdbc "0.7.8"]
                 [honeysql "0.9.6"]]

  :profiles

  {:uberjar {:aot :all}

   :dev {:dependencies
         [[org.clojure/clojure "1.10.0"]

          ;; formatting
          [org.hibernate/hibernate-core "5.4.6.Final"
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
            org.jboss.spec.javax.transaction/jboss-transaction-api_1.2_spec]]

          [org.postgresql/postgresql "42.1.3"]
          [com.datomic/datomic-free "0.9.5697"]]}}

  :target-path "target/%s")
