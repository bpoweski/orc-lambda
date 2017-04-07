(ns orc-lambda.core
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [cheshire.core :as json]
            [clojure.walk :as walk]
            [clj-time.core :as time]
            [clj-time.format :as fmt]
            [puget.printer :as puget]
            [orca.core :as orca]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import (org.xerial.snappy SnappyFramedInputStream)))


(defn cprint [x]
  (puget/pprint x {:print-color true}))

(defn parse-date-time [in]
  (fmt/parse (fmt/formatter time/utc :date-time-no-ms :date-time) in))

(defn parse-date* [in]
  (fmt/parse-local-date (fmt/formatter :year-month-day) in))

(def parse-date (memoize parse-date*))

(defn parse-utc-date [in]
  (fmt/parse (fmt/formatter time/utc :year-month-day :date-hour) in))

(defn parse-request [input]
  (-> input
      (json/parse-string keyword)
      (update :requested_at parse-date-time)
      (update-in [:room_rate_request :date] parse-date)))

(defn flatten-request [request]
  (-> request
      (dissoc :room_rate_request)
      (merge (:room_rate_request request))
      (update :rooms #(map :ages %))
      (set/rename-keys {:date :check_in :rooms :rinfo})))

(defn snappy-input-stream [file]
  (SnappyFramedInputStream. (io/input-stream file)))

(defn read-lines [in]
  (with-open [reader (io/reader in)]
    (vec (line-seq reader))))

(def schema
  (str/join
   ["struct<lowest_by_restriction:boolean,room_type_id:string,point_of_sale:string,cancellation_policies:boolean,display_currency:string,"
    "property_uuid:array<string>,currency:string,nights:tinyint,affiliate_code:smallint,request_initiator:string,audit_info:boolean,"
    "cache_control:string,check_in:date,rate_plan_code:string,session_token:string,timeout:tinyint,requested_at:timestamp,"
    "rinfo:array<array<tinyint>>,request_id:string>"]))

(defn process-request [json]
  (-> json
      parse-request
      flatten-request))

(defn -main [& args]
  (doseq [file args]
    (println file)
    (with-open [in (snappy-input-stream file)]
      (let [requests (map process-request (read-lines in))]
        (cprint (orca/type-description (orca/rows->schema (take 1024 requests))))
        (orca/write-rows "example.orc" requests schema :overwrite? true)))))
