(ns orca.core
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.data :refer [diff]]
            [clojure.core.unify :as unify])
  (:import [org.apache.hadoop.hive.ql.exec.vector ColumnVector LongColumnVector]
           [org.apache.orc OrcFile Reader Writer]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path]
           [org.apache.hadoop.hive.serde2.io HiveDecimalWritable]))


(defn to-path [x]
  (cond
    (instance? java.net.URL x) (Path. (.toURI x))
    (instance? Path x)         x))

(defn file-reader
  "Creates an ORC reader for a given file or path."
  [path]
  (OrcFile/createReader (to-path path) (OrcFile/readerOptions (Configuration.))))

(defn file-writer
  "Creates an ORC writer for a given file or path."
  [path]
  (OrcFile/createWriter (to-path path) (OrcFile/writerOptions (Configuration.))))

(defprotocol ColumnVectorDecoder
  (decode-vec! [col coll nrows]))

(defn decode-column
  "Decodes a persistent vector from a column vector."
  [^ColumnVector v]
  (persistent! (decode-vec! v (transient []) (alength (.vector v)))))

(defn append-batch! [frame batch]
  (let [indexed-cols (map-indexed vector (.cols batch))
        nrows (.size batch)]
    (loop [frame frame
           [[i chunk] & more] indexed-cols]
      (let [frame (assoc frame i (decode-vec! chunk (get frame i (transient [])) nrows))]
        (if (seq more)
          (recur frame more)
          frame)))))

(defn read-rows
  "Synchrounously reads rows from input."
  ([input] (read-rows input {}))
  ([input options]
   (let [reader        (file-reader (to-path input))
         schema        (.getSchema reader)
         batch         (.createRowBatch schema)
         record-reader (.rows reader)]
     (loop [frame {}]
       (if (.nextBatch record-reader batch)
         (recur (append-batch! frame batch))
         (mapv persistent! (vals frame)))))))

(extend-protocol ColumnVectorDecoder
  org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
  (decode-vec! [^DecimalColumnVector arr result nrows]
    (loop [idx 0
           result result]
      (if (< idx nrows)
        (let [^HiveDecimalWritable d (aget (.vector arr) idx)
              result (conj! result (if (or (.noNulls arr) (not (aget (.isNull arr) idx)))
                                     (.bigDecimalValue (.getHiveDecimal d))
                                     nil))]
          (recur (inc idx) result))
        result)))

  org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
  (decode-vec! [^LongColumnVector arr result nrows]
    (loop [idx 0
           result result]
      (if (< idx nrows)
        (let [result (conj! result (if (or (.noNulls arr) (not (aget (.isNull arr) idx)))
                                     (aget (.vector arr) idx)
                                     nil))]
          (recur (inc idx) result))
        result))))

(defprotocol TypeInference
  (data-type [v])
  (data-props [v]))

(derive ::array ::compound)
(derive ::map ::compound)
(derive ::struct ::compound)
(derive ::union ::compound)

(extend-protocol TypeInference
  (Class/forName "[C")
  (data-type [v] ::char)
  (data-props [v] {:length (alength v)}))

(extend-protocol TypeInference
  ;; array      ListColumnVector
  java.util.List
  (data-type [v] ::array)
  (data-props [v])

  ;; binary     BytesColumnVector

  ;; bigint     LongColumnVector
  java.math.BigInteger
  (data-type [v] ::bigint)
  (data-props [v])

  ;; boolean    LongColumnVector
  java.lang.Boolean
  (data-type [v] ::boolean)
  (data-props [v])

  ;; char       BytesColumnVector
  java.lang.Character
  (data-type [v] ::char)
  (data-props [v] {:length 1})

  ;; date       LongColumnVector
  java.util.Date
  (data-type [v] ::date)
  (data-props [v])

  ;; decimal    DecimalColumnVector
  java.math.BigDecimal
  (data-type [v] ::decimal)
  (data-props [v] {:scale (.scale v) :precision (.precision v)})

  ;; float      DoubleColumnVector
  java.lang.Float
  (data-type [v] ::float)
  (data-props [v])

  ;; double     DoubleColumnVector
  java.lang.Double
  (data-type [v] ::double)
  (data-props [v])

  ;; int        LongColumnVector
  ;; long       LongColumnVector
  ;; smallint   LongColumnVector
  ;; tinyint    LongColumnVector
  java.lang.Number
  (data-type [v]
    (let [x (long v)]
      (cond
        (>= x Byte/MIN_VALUE)    (cond
                                   (<= x Byte/MAX_VALUE)    ::tinyint
                                   (<= x Short/MAX_VALUE)   ::smallint
                                   (<= x Integer/MAX_VALUE) ::int
                                   :else ::bigint)
        (>= x Short/MIN_VALUE)   ::smallint
        (>= x Integer/MIN_VALUE) ::int
        :else                    ::bigint)))
  (data-props [v])

  ;; map        MapColumnVector
  java.util.Map
  (data-type [v] ::map)
  (data-props [v])

  ;; struct     StructColumnVector

  ;; timestamp  TimestampColumnVector

  ;; uniontype  UnionColumnVector

  ;; string     BytesColumnVector
  ;; varchar    BytesCoumnVector
  java.lang.String
  (data-type [v] ::string)
  (data-props [v] {:length (.length v)})

  nil
  (data-type [v] nil)
  (data-props [v]))

(defn stats [coll]
  (let [nrows (count coll)
        coll  (remove nil? coll)]
    {:sum   (reduce + coll)
     :min   (apply min coll)
     :max   (apply max coll)
     :count nrows}))

(defn infer [m val]
  (let [inferred (data-type val)
        existing (get-in m [:type-freq inferred] 0)]
    (assoc-in m [:type-freq inferred] (inc existing))))

(defn make-column-vector [v {:keys [type-freq] :as column-meta}]
  (let [nils? (pos? (get type-freq ::nil 0))]
    ))

(defn vectorize
  "Buffers values and then emits a ColumnnVector of the discovered type."
  ([]
   (fn [rf]
     (let [a           (java.util.ArrayList.)
           column-meta (volatile! {})
           clear!      (fn []
                         (.clear a)
                         (vreset! column-meta {}))
           track!      (fn [input]
                         (vswap! column-meta infer input)
                         (.add a input))]
       (fn
         ([] (rf))
         ([result]
          (let [col-meta @column-meta
                result   (if (.isEmpty a)
                           result
                           (let [col-vec (make-column-vector (vec (.toArray a)) @column-meta)]
                             ;;clear first!
                             (clear!)
                             (unreduced (rf result col-vec))))]
            (pprint col-meta)
            (rf result)))
         ([result input]
          (if (< (count a) 1024)
            (track! input)
            (let [col-vec (make-column-vector (vec (.toArray a)) @column-meta)]
              (clear!)
              (let [ret (rf result col-vec)]
                (when-not (reduced? ret)
                  (track! input))
                ret))))))))
  ([coll] (sequence (vectorize) coll)))

(defn make-column-vector* [coll]
  (let [v   (LongColumnVector.)
        arr (long-array (count coll))
        _   (set! (.vector v) arr)
        _   (set! (.noNulls v) true)]
    (doseq [[idx x] (map-indexed vector coll)]
      (if (nil? x)
        (do (set! (.noNulls v) false)
            (aset-boolean (.isNull v) idx true))
        (aset-long arr idx x)))
    v))

(defn encode-column [coll]
  (let [x (first coll)]
    (condp instance? x
      Long   (make-column-vector* coll)
      String (make-column-vector* coll))))

(defn parse-file []
  (cheshire.core/parse-string (slurp (io/resource "search.json")) keyword))

;; (defn infer-typedef [x]
;;   (let [map-reduce   (fn [ret k v] ;; har har
;;                        (let [dt      (data-type v)
;;                              typedef (if-let [props (data-props v)]
;;                                        [k dt props]
;;                                        [k dt])]
;;                          (conj ret typedef)))
;;         array-reduce (fn [ret k v] ;; har har
;;                        (let [dt      (data-type v)
;;                              typedef (if-let [props (data-props v)]
;;                                        [k dt props]
;;                                        [k dt])]
;;                          (conj ret typedef)))]
;;     (case (data-type x)
;;       ::map (reduce-kv map-reduce [] x))
;;     ))

(defmulti typedef data-type)

(defmethod typedef :default [x]
  [(data-type x) (or (data-props x) {})])

(defmethod typedef ::map [x]
  [::map
   (reduce-kv
    (fn [kmap k v]
      (assoc kmap k (typedef v)))
    {}
    x)])

(defmethod typedef ::array [x]
  (let [child-types (set (map typedef (remove nil? x)))
        n-types     (count child-types)
        tdef        [::array {:null? (boolean (seq (filter nil? x)))}]]
    (cond
      (zero? n-types) tdef
      (= n-types 1)   (conj tdef (first child-types))
      :else           (conj tdef child-types))))


;; (derive ::array ::compound)
;; (derive ::map ::compound)
;; (derive ::struct ::compound)
;; (derive ::union ::compound)

(defn schema
  ([x] x)
  ([x y]
   (reduce-kv
    (fn [ret idx v]
      ret)
    x
    y)))

(defn rows->schema [rows]
  (->> rows
       (map typedef)
       (reduce schema)))
