(ns domic.transact-test
  (:require
   [clojure.test
    :refer [is deftest testing use-fixtures
            run-tests]]

   [domic.fixtures :refer [fix-test-db *scope*]]
   [domic.engine :as en]
   [domic.api :as api]))


(use-fixtures :once fix-test-db)


(deftest test-foo)
