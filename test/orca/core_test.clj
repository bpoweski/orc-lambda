(ns orca.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [orca.core :as orca :refer :all])
  (:import [org.apache.hadoop.fs Path]))


(deftest path-test
  (is (= (Path. (.toURI (io/resource "decimal.orc"))) (to-path (io/resource "decimal.orc")))))

(deftest orc-reader-test
  (let [[stats & _] (map stats (read-rows (io/resource "decimal.orc")))]
    (is (= 6000 (:count stats)))
    (is (= 1999.2M (:max stats)))
    (is (= -1000.5M (:min stats)))
    (is (= 1998301.099M (:sum stats)))))

(deftest encoder-column-test
  (testing "vector with longs"
    (is (= [1 2 3] (decode-column (encode-column [1 2 3]))))
    (is (= [1 nil 3] (decode-column (encode-column [1 nil 3])))))
  ;; (testing "vector of strings"
  ;;   (is (= ["dog" "cat" nil] (decode-column (vectorize ["dog" "cat" nil])))))
  )

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
    (is (= ::orca/char (data-type (char-array [\f \o \o]))))))

(deftest typedef-test
  (testing "Arrays"
    (is (= [::orca/array {:null? false} [::orca/tinyint {}]] (typedef [1])))
    (is (= [::orca/array {:null? false} [::orca/tinyint {}]] (typedef [1 -1])))
    (is (= [::orca/array {:null? true} [::orca/tinyint {}]] (typedef [1 nil])))
    (is (= [::orca/string {:length 3}] (typedef "foo"))))
  (testing "Arrays of compound types"
    (is (= [:orca.core/array
            {:null? false}
            #{[:orca.core/map {:a [:orca.core/tinyint {}]}]
              [:orca.core/map
               {:a [:orca.core/smallint {}],
                :b [:orca.core/string {:length 3}]}]
              [:orca.core/map {:a [:orca.core/smallint {}]}]}]
           (typedef [{:a 1} {:a 10000} {:a 10001 :b "foo"}]))))
  (testing "Map"
    (is (= [::orca/map
            {:a    [::orca/tinyint {}]
             "foo" [::orca/string {:length 3}]
             10    [::orca/tinyint {}]}]
           (typedef {:a 1 "foo" "bar" 10 11})))))

(deftest combine-test
  (testing ""
    ))
