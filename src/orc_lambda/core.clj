(ns orc-lambda.core
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [cheshire.core :as json]
            [clojure.walk :as walk]
            [clj-time.core :as time]
            [clj-time.format :as fmt])
  (:import [org.apache.hadoop.hive.ql.exec.vector ColumnVector LongColumnVector]
           [org.apache.orc OrcFile Reader Writer]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path]
           [org.apache.hadoop.hive.serde2.io HiveDecimalWritable]))


(defn to-path [x]
  (cond
    (instance? java.net.URL x) (Path. (.toURI x))
    (instance? Path x)         x))

(defn file-writer
  "Creates an ORC writer for a given file or path."
  [path]
  (OrcFile/createWriter (to-path path) (OrcFile/writerOptions (Configuration.))))
