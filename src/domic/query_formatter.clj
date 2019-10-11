(ns domic.query-formatter
  (:require
   [domic.error :as e]))


(def api-url "https://sqlformat.org/api/v1/format")


(defonce request-fn
  (do
    (try
      (require '[clj-http.client :as client])
      (catch Exception e))
    (resolve 'client/request)))


(defn format-query
  [query]
  (if request-fn

    (-> {:url api-url
         :method :post
         :form-params {:sql query
                       :reindent 1
                       :indent_width 4
                       :keyword_case "upper"}
         :as :json}
        (request-fn)
        :body
        :result)

    (e/error!
     (str "Warning: clj-http.client library is required "
          "as we format queries using sqlformat.org API. "
          "Add clj-http.client and cheshire libraries "
          "into your dev dependencies and restart the REPL."))))
