(ns orca.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [orca.core :as orca :refer :all]
            [clj-time.core :as time])
  (:import [org.apache.hadoop.fs Path]))


(deftest path-test
  (is (= (Path. (.toURI (io/resource "decimal.orc"))) (to-path (io/resource "decimal.orc")))))

(deftest orc-reader-test
  (let [[stats & _] (map stats (vals (read-vectors (io/resource "decimal.orc"))))]
    (is (= 6000 (:count stats)))
    (is (= 1999.2M (:max stats)))
    (is (= -1000.5M (:min stats)))
    (is (= 1998301.099M (:sum stats)))))

(deftest type-inference-test
  (testing "BigDecimal"
    (is (= ::orca/decimal (data-type 10.0M)))
    (is (= {:scale 1 :precision 3} (data-props 10.0M))))
  (testing "Boolean"
    (is (= ::orca/boolean (data-type true)))
    (is (= ::orca/boolean (data-type false))))
  (testing "Integer"
    (is (= ::orca/tinyint (data-type 10)))
    (is (= ::orca/tinyint (data-type 0)))
    (is (= ::orca/smallint (data-type Short/MAX_VALUE)))
    (is (= ::orca/smallint (data-type Short/MIN_VALUE)))
    (is (= ::orca/int (data-type (inc Short/MAX_VALUE))))
    (is (= ::orca/int (data-type (dec Short/MIN_VALUE))))
    (is (= ::orca/bigint (data-type (inc Integer/MAX_VALUE))))
    (is (= ::orca/bigint (data-type (dec Integer/MIN_VALUE)))))
  (testing "Float"
    (is (= ::orca/float (data-type (float -1.0))))
    (is (= ::orca/float (data-type Float/MAX_VALUE)))
    (is (= ::orca/float (data-type Float/MIN_VALUE))))
  (testing "Double"
    (is (= ::orca/double (data-type -1.0)))
    (is (= ::orca/double (data-type 100.00))))
  (testing "String"
    (is (= ::orca/string (data-type "")))
    (is (= ::orca/string (data-type "foo"))))
  (testing "Char"
    (is (= ::orca/char (data-type \newline)))
    (is (= ::orca/char (data-type (char-array [\f \o \o])))))
  (testing "DateTime"
    (is (= ::orca/timestamp (data-type (time/date-time 2017 4 3 10)))))
  (testing "Date"
    (is (= ::orca/date (data-type (time/local-date 2017 4 3))))))

(deftest typedef-test
  (testing "Arrays"
    (is (= [::orca/array [::orca/tinyint]] (typedef [1])))
    (is (= [::orca/array [::orca/tinyint]] (typedef [1 -1])))
    (is (= [::orca/array [::orca/tinyint]] (typedef [1 nil])))
    (is (= [::orca/string] (typedef "foo"))))
  (testing "Arrays of compound types"
    (is (= [::orca/array
            #{[::orca/struct {:a [::orca/tinyint]}]
              [::orca/struct
               {:a [::orca/smallint],
                :b [::orca/string]}]
              [::orca/struct {:a [::orca/smallint]}]}]
           (typedef [{:a 1} {:a 10000} {:a 10001 :b "foo"}]))))
  (testing "Map"
    (is (= [::orca/struct
            {:a    [::orca/tinyint]
             "foo" [::orca/string]
             10    [::orca/tinyint]}]
           (typedef {:a 1 "foo" "bar" 10 11})))))

(deftest type-description-test
  (testing "numeric"
    (is (= "tinyint" (infer-typedesc 1)))
    (is (= "smallint" (infer-typedesc 128)))
    (is (= "int" (infer-typedesc (inc Short/MAX_VALUE))))
    (is (= "bigint" (infer-typedesc (inc Integer/MAX_VALUE))))
    (is (= "float" (infer-typedesc (float 1))))
    (is (= "double" (infer-typedesc 1.0))))
  (testing "strings"
    (is (= "string" (infer-typedesc "hello"))))
  (testing "decimals"
    (is (= "decimal(2,1)" (infer-typedesc 1.0M))))
  ;; (testing "map"
  ;;   (is (= "map<tinyint,string>" (infer-typedesc {10 "foo"}))))
  (testing "struct"
    (is (= "struct<k:string,y:boolean>" (infer-typedesc {:k "foo" :y true}))))
  (testing "date"
    (is (= "date" (infer-typedesc (time/local-date 2017 1 1)))))
  (testing "timestamp"
    (is (= "timestamp" (infer-typedesc (time/date-time 2017 1 1))))))

(deftest merge-schema-test
  (testing "combining two different types yields a union"
    (is (= [::orca/array [::orca/union #{[::orca/tinyint] [::orca/int]}]]
           (merge-schema (typedef [1]) (typedef [Integer/MAX_VALUE])))))
  (testing "two different untions"
    (is (= [::orca/union #{[::orca/tinyint] [::orca/string] [::orca/boolean]}]
           (merge-schema [::orca/union #{[::orca/string] [::orca/boolean]}] [::orca/union #{[::orca/boolean] [::orca/tinyint]}]))))
  (testing "union vs non-union primitive"
    (is (= [::orca/union #{[::orca/tinyint] [::orca/string]}]
           (merge-schema [::orca/union #{[::orca/tinyint] [::orca/string]}] [::orca/string]))))
  (testing "merging two structs"
    (is (= [::orca/struct {:x [::orca/tinyint] :y [::orca/boolean]}]
           (merge-schema [::orca/struct {:x [::orca/tinyint]}] [::orca/struct {:y [::orca/boolean]}])))))

(defn roundtrip [input schema]
  (let [tmp    (tmp-path)
        _      (write-rows tmp input schema)]
    (try
      (read-vectors tmp)
      (finally
        (.delete (io/file tmp))))))

(deftest round-trip-test
  (testing "single vector"
    (let [in [[1] [2] [3]]]
      (is (= in (frame->vecs (roundtrip in "struct<x:int>"))))))
  (testing "two vectors of different types"
    (let [in [[1 "a"] [2 "b"]]]
      (is (= in (frame->vecs (roundtrip in "struct<x:int,y:string>"))))))
  (testing "two vectors with nils"
    (let [out (roundtrip [[nil "a"] [2 nil]] "struct<x:int,y:string>")]
      (is (= {:x [nil 2] :y ["a" nil]} out))))
  (testing "writing map into a struct"
    (let [out (roundtrip [{:x "foo" :y 10} {:x "bar" :y 100000} {:z false}] "struct<x:string,y:int>")]
      (is (= {:x ["foo" "bar" nil] :y [10 100000 nil]} out)))))
