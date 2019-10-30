(ns jepsen.etcd.client
  "Client library wrapper for jetcd"
  (:refer-clojure :exclude [get])
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.etcd [support :as support]]
            [jepsen.etcd.client.txn :as t]
            [jepsen.util :refer [coll]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.lang AutoCloseable)
           (java.nio.charset Charset)
           (io.etcd.jetcd ByteSequence
                          Client
                          ClientBuilder
                          CloseableClient
                          KeyValue
                          KV
                          Response)
           (io.etcd.jetcd.kv GetResponse
                             PutResponse
                             TxnResponse)
           (io.etcd.jetcd.op Cmp
                             Cmp$Op
                             Op
                             Op$PutOp
                             Op$GetOp)
           (io.grpc Status$Code
                    StatusRuntimeException)))
; Serialization

(def ->bytes t/->bytes)
(def bytes-> t/bytes->)

; Coercing responses to Clojure data
(defprotocol ToClj
  (->clj [o]))

(extend-protocol ToClj
  nil          (->clj [x] x)

  ByteSequence (->clj [bs] (bytes-> bs))

  KeyValue (->clj [kv]
             (clojure.lang.MapEntry. (->clj (.getKey kv))
                                     {:value            (->clj (.getValue kv))
                                      :version          (.getVersion kv)
                                      :create-revision  (.getCreateRevision kv)
                                      :mod-revision     (.getModRevision kv)}))

  GetResponse (->clj [r]
                {:count  (.getCount r)
                 :kvs    (into {} (map ->clj (.getKvs r)))
                 :more?  (.isMore r)})

  PutResponse (->clj [r]
                {:prev-kv (->clj (.getPrevKv r))
                 :prev-kv? (.hasPrevKv r)})

  TxnResponse (->clj [r]
                {:succeeded? (.isSucceeded r)
                 :gets       (map ->clj (.getGetResponses r))
                 :puts       (map ->clj (.getPutResponses r))
                 :txns       (map ->clj (.getTxnResponses r))})
  )

; Opening and closing clients

(defn ^Client client
  "Builds a client for the given node."
  [node]
  (.. (Client/builder)
      (endpoints (into-array String [(support/client-url node)]))
      (lazyInitialization false)
      ; (loadBalancerPolicy "some string???")
      (build)))

(defn close!
  "Closes any client"
  [^AutoCloseable c]
  (.close c))

; Error handling

(defn original-cause
  "Unwraps throwables to return their original cause."
  [^Throwable t]
  (if-let [cause (.getCause t)]
    (recur cause)
    t))

(defmacro unwrap-exceptions
  "GRPC likes to wrap exceptions in a bunch of ExecutionException wrappers.
  Let's catch those and unwrap them to throw the original exception."
  [& body]
  `(try ~@body
       (catch java.util.concurrent.ExecutionException e#
         (throw (original-cause e#)))))

(defn status-exception->op
  "Takes a status exception, an op, and a set of idempotent op :f's. Returns
  op with an appropriate :type (e.g. :info, :fail), and an :error for
  recognized statuses."
  [e op idempotent]
  (let [crash  (if (idempotent (:f op)) :fail :info)
        status (.getStatus e)
        desc   (.getDescription status)]
    ; lmao, can't use a case statement here for... reasons
    (condp = (.getCode status)
      Status$Code/UNAVAILABLE
      (assoc op :type crash, :error [:unavailable desc])

      ; Fall back to regular expressions on status messages
      (do (info "Unknown error status code" (.getCode status) "-" status "-" e)
          (condp re-find (.getMessage e)
            (throw e))))))

(defmacro with-errors
  "Takes an operation, a set of op types which are idempotent, and evals body,
  converting known exceptions to :fail or :info return ops."
  [op idempotent & body]
  `(try (unwrap-exceptions ~@body)
        (catch StatusRuntimeException e#
          (status-exception->op e# ~op ~idempotent))))

; KV ops

(defn ^KV kv-client
  "Extracts a KV client from a client."
  [^Client c]
  (.getKVClient c))

(defn put!
  "Sets key to value, synchronously."
  [c k v]
  (-> c kv-client
      (.put (->bytes k) (->bytes v))
      .get
      ->clj))

(defn get
  "Gets the value for a key, synchronously."
  [c k]
  (-> c kv-client
      (.get (->bytes k))
      .get
      ->clj
      :kvs
      vals
      first))

(defn txn!
  "Right now, transactions are all if statements, so this takes 2 or three
  arguments: a test (which may be a collection) of guard ops, a true branch,
  and a false branch. Branches may be single ops or collections of ops. With
  only two args, skips the guard expr."
  ([c t-branch]
   (txn! c nil t-branch))
  ([c test t-branch]
   (txn! c test t-branch nil))
  ([c test t f]
   (info :txn :test test :true-branch t)
   (->clj
     (.. (kv-client c)
         (txn)
         (If   (into-array Cmp (coll test)))
         (Then (into-array Op  (coll t)))
         (Else (into-array Op  (coll f)))
         (commit)
         (get)))))

(defn cas!
  "A compare-and-set transaction on key k from value v to v'. Returns false if
  failed, true otherwise."
  [c k v v']
  (let [r (-> c
              (txn! (t/= k (t/value v))
                    (t/put k v')))]
    (info :cas! k :from v :to v' :res r)
    (:succeeded? r)))
