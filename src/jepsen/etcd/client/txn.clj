(ns jepsen.etcd.client.txn
  "Support functions for constructing transactional expressions."
  (:refer-clojure :exclude [get < = > compare])
  (:require [jepsen [codec :as codec]])
  (:import (java.nio.charset Charset)
           (io.etcd.jetcd ByteSequence)
           (io.etcd.jetcd.options GetOption
                                  PutOption)
           (io.etcd.jetcd.op Cmp
                             Cmp$Op
                             CmpTarget
                             Op
                             Op$PutOp
                             Op$GetOp)))

(def ^Charset utf8 (Charset/forName "UTF-8"))

(defn ^ByteSequence str->bytes
  "Converts a string to bytes, as UTF-8"
  [^String s]
  (ByteSequence/from (str s) utf8))

(defn bytes->str
  [^ByteSequence bs]
  (.toString bs utf8))

(defn ^ByteSequence ->bytes
  "Coerces any object to a ByteSequence"
  [x]
  (if (instance? ByteSequence x)
    x
    (ByteSequence/from (codec/encode x))))

(defn bytes->
  "Coerces a ByteSequence to any object"
  [^ByteSequence bs]
  (codec/decode (.getBytes bs)))

(def put-option-with-prev-kv
  "A put option which returns the previous kv"
  (.. (PutOption/newBuilder) withPrevKV build))

; Transactional Ops
(defn ^Op$GetOp get
  "Constructs a get operation."
  [k]
  (Op/get (->bytes k) GetOption/DEFAULT))

(defn ^Op$PutOp put
  "Constructs a put operation."
  [k v]
  (Op/put (->bytes k) (->bytes v) put-option-with-prev-kv))

(defn version
  "A comparison target by version."
  [^long v]
  (CmpTarget/version v))

(defn value
  "A comparison target by value."
  [v]
  (CmpTarget/value (->bytes v)))

(defn mod-revision
  "A comparison target by modification revision."
  [^long revision]
  (CmpTarget/modRevision revision))

(defn create-revision
  "A comparison target by creation revision."
  [^long revision]
  (CmpTarget/createRevision revision))

(defn =
  "Constructs an equality comparison between key and target."
  [k target]
  (Cmp. (->bytes k) Cmp$Op/EQUAL target))

(defn <
  "Constructs an LESS inequality comparison between key and target."
  [k target]
  (Cmp. (->bytes k) Cmp$Op/LESS target))

(defn >
  "Constructs an GREATER inequality comparison between key and target."
  [k target]
  (Cmp. (->bytes k) Cmp$Op/GREATER target))
