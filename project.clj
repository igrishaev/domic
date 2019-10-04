(defproject domic "0.1.0-SNAPSHOT"

  :description "FIXME: write description"

  :url "http://example.com/FIXME"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [
                 [org.clojure/java.jdbc "0.7.8"]
                 [honeysql "0.9.6"]

                 #_[cheshire "5.6.3"]
                 #_[org.clojars.akiel/datomic-spec "0.5.2"]
                 #_[com.datomic/datomic-pro "0.9.5561.50"]
                 ]

  :profiles

  {:uberjar {:aot :all}

   :dev {:dependencies
         [[org.clojure/clojure "1.10.0"]
          [org.postgresql/postgresql "42.1.3"]
          [com.datomic/datomic-free "0.9.5697"]]}}

  :target-path "target/%s")
