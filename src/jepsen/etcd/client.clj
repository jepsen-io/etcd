(ns jepsen.etcd.client
  "Client library wrapper for jetcd"
  (:refer-clojure :exclude [await get swap!])
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.etcd [support :as support]]
            [jepsen.etcd.client.txn :as t]
            [jepsen.util :refer [coll]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.lang AutoCloseable)
           (java.net URI)
           (java.nio.charset Charset)
           (io.etcd.jetcd ByteSequence
                          Client
                          ClientBuilder
                          CloseableClient
                          Cluster
                          KeyValue
                          KV
                          Lease
                          Lock
                          Maintenance
                          Response
                          Response$Header)
           (io.etcd.jetcd.common.exception ClosedClientException)
           (io.etcd.jetcd.cluster Member
                                  MemberListResponse)
           (io.etcd.jetcd.kv GetResponse
                             PutResponse
                             TxnResponse)
           (io.etcd.jetcd.lease LeaseGrantResponse
                                LeaseKeepAliveResponse
                                LeaseRevokeResponse)
           (io.etcd.jetcd.lock LockResponse
                               UnlockResponse)
           (io.etcd.jetcd.maintenance StatusResponse)
           (io.etcd.jetcd.op Cmp
                             Cmp$Op
                             Op
                             Op$PutOp
                             Op$GetOp)
           (io.etcd.jetcd.options GetOption
                                  PutOption)
           (io.grpc Status$Code
                    StatusRuntimeException)
           (io.grpc.stub StreamObserver)))

(def timeout
  "A default timeout, in ms."
  5000)

; Serialization

(def ->bytes t/->bytes)
(def bytes-> t/bytes->)

; Coercing responses to Clojure data
(defprotocol ToClj
  (->clj [o]))

(extend-protocol ToClj
  nil          (->clj [x] x)

  ByteSequence (->clj [bs] (bytes-> bs))

  GetResponse (->clj [r]
                {:count  (.getCount r)
                 :kvs    (into {} (map ->clj (.getKvs r)))
                 :more?  (.isMore r)
                 :header (->clj (.getHeader r))})

  KeyValue (->clj [kv]
             (clojure.lang.MapEntry. (->clj (.getKey kv))
                                     {:value            (->clj (.getValue kv))
                                      :version          (.getVersion kv)
                                      :create-revision  (.getCreateRevision kv)
                                      :mod-revision     (.getModRevision kv)}))

  LeaseGrantResponse (->clj [r]
                       {:header (->clj (.getHeader r))
                        :id     (.getID r)
                        :ttl    (.getTTL r)})

  LeaseKeepAliveResponse (->clj [r]
                           {:header (->clj (.getHeader r))
                            :id     (.getID r)
                            :ttl    (.getTTL r)})

  LeaseRevokeResponse (->clj [r]
                        {:header (->clj (.getHeader r))})

  LockResponse (->clj [r]
                 {:header (->clj (.getHeader r))
                  :key    (.getKey r)})

  Member (->clj [r]
           {:name (.getName r)
            :id   (.getId r)})

  MemberListResponse (->clj [r]
                       {:header (->clj (.getHeader r))
                        :members (map ->clj (.getMembers r))})

  PutResponse (->clj [r]
                {:prev-kv   (->clj (.getPrevKv r))
                 :prev-kv?  (.hasPrevKv r)
                 :header    (->clj (.getHeader r))})

  Response$Header (->clj [h]
                    {:member-id (.getMemberId h)
                     :revision  (.getRevision h)
                     :raft-term (.getRaftTerm h)})

  StatusResponse (->clj [r]
                   {:db-size      (.getDbSize r)
                    :leader       (.getLeader r)
                    :raft-index   (.getRaftIndex r)
                    :raft-term    (.getRaftTerm r)
                    :version      (.getVersion r)
                    :header       (->clj (.getHeader r))})

  TxnResponse (->clj [r]
                {:succeeded? (.isSucceeded r)
                 :gets       (map ->clj (.getGetResponses r))
                 :puts       (map ->clj (.getPutResponses r))
                 :txns       (map ->clj (.getTxnResponses r))
                 :header     (->clj (.getHeader r))})

  UnlockResponse (->clj [r]
                   {:header (->clj (.getHeader r))})
  )

; Opening and closing clients

(defn ^Client client
  "Builds a client for the given node."
  [node]
  (.. (Client/builder)
      (endpoints (into-array String [(support/client-url node)]))
      ;(endpoints (into-array String (map support/client-url ["n1" "n2" "n3" "n4" "n5"])))
      (lazyInitialization false)
      ; (loadBalancerPolicy "some string???")
      (build)))

(defn close!
  "Closes any client. Ignores ClosedClientExceptions."
  [^AutoCloseable c]
  (try
    (.close c)
    (catch ClosedClientException e
      :already-closed)))

; Futures
(defn await
  "Derefences a future, using a default timeout, and throwing a slingshot
  exception of :type :timeout if it times out."
  [future]
  (let [r (deref future timeout ::timeout)]
    (if (= ::timeout r)
      (throw+ {:type :timeout})
      r)))

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

      Status$Code/NOT_FOUND
      (assoc op :type :fail, :error [:not-found desc])

      Status$Code/UNKNOWN
      (condp re-find desc
        #"leader changed" (assoc op :type crash, :error :leader-changed)
        (do (info "Unknown code=UNKNOWN description" (pr-str desc))
            (throw e)))

      ; Fall back to regular expressions on status messages
      (do (info "Unknown error status code" (.getCode status) "-" status "-" e)
          (condp re-find (.getMessage e)
            (throw e))))))

(defmacro with-errors
  "Takes an operation, a set of op types which are idempotent, and evals body,
  converting known exceptions to :fail or :info return ops."
  [op idempotent & body]
  `(try+ (unwrap-exceptions ~@body)
         (catch StatusRuntimeException e#
           (status-exception->op e# ~op ~idempotent))
         (catch java.net.ConnectException e#
           (assoc ~op :type :fail, :error [:connect-timeout (.getMessage e#)]))
         (catch [:type :timeout] e#
           (assoc ~op
                  :type (if (~idempotent (:f ~op)) :fail :info)
                  :error :timeout))))

; KV ops

(defn ^KV kv-client
  "Extracts a KV client from a client."
  [^Client c]
  (.getKVClient c))

(defn put!
  "Sets key to value, synchronously."
  [c k v]
  (-> c kv-client
      (.put (->bytes k) (->bytes v) t/put-option-with-prev-kv)
      await
      ->clj))

(defn get-options
  "Takes an option map for a get request, and constructs a GetOption. Options
  are:

      :serializable?      true/false"
  [opts]
  (.. (GetOption/newBuilder)
      (withSerializable (boolean (:serializable? opts)))
      (build)))

(defn get*
  "Gets the value for a key, synchronously. Raw version; includes headers and
  full response."
  ([c k]
   (get* c k {}))
  ([c k opts]
   (-> c kv-client
       (.get (->bytes k) (get-options opts))
       await
       ->clj)))

(defn get
  "Gets the value for a key, synchronously. Friendly version: returns just the
  key's value."
  ([c k]
   (get c k {}))
  ([c k opts]
   (-> (get* c k opts)
       :kvs
       vals
       first)))

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
   (-> (.. (kv-client c)
           (txn)
           (If   (into-array Cmp (coll test)))
           (Then (into-array Op  (coll t)))
           (Else (into-array Op  (coll f)))
           (commit))
       await
       ->clj)))

(defn cas*!
  "Like cas!, but raw; returns full txn response map."
  [c k v v']
  (txn! c
        (t/= k (t/value v))
        (t/put k v')))

(defn cas!
  "A compare-and-set transaction on key k from value v to v'. Returns false if
  failed, true otherwise."
  [c k v v']
  (-> c
      (cas*! k v v')
      :succeeded?))

(defn cas-revision!
  "Like cas!, but takes a current modification revision for the key, and
  updates it to v' only if the revision matches."
  [c k rev v']
  (-> c
      (txn! (t/= k (t/mod-revision rev))
            (t/put k v'))
      :succeeded?))

(defn swap-retry-delay
  "A delay time for swap! retries, in milliseconds"
  []
  (rand 50))

(defn swap!
  "Like clojure.core's swap!; takes a key and a function taking a value of that
  key to a new value. Calls cas! to update the value of that key to (f
  current-val & args). Includes a retry loop."
  [c k f & args]
  (loop []
    (let [{:keys [value mod-revision]} (get c k)
          value' (apply f value args)]
      (if (cas-revision! c k mod-revision value')
        value'
        (do (Thread/sleep (swap-retry-delay))
            (recur))))))

(defn ^Lease lease-client
  "Gets a lease client from a client."
  [^Client c]
  (.getLeaseClient c))

(defn grant-lease!
  "Grants a lease on a client, with the given TTL in seconds."
  [c ^long ttl]
  (-> c lease-client (.grant ttl) await ->clj))

(defn revoke-lease!
  "Revokes a lease."
  [c ^long lease-id]
  (-> c lease-client (.revoke lease-id) await ->clj))

(defn keep-lease-alive!
  "Keeps a lease alive forever. Returns a ClosableClient. I don't really think
  this means FOREVER, but the API docs are super unclear. I assume we call
  .close to stop sending keepalives?"
  [c ^long lease-id]
  (let [observer (reify StreamObserver
                   (onNext [this v]     (info :onNext lease-id (->clj v)))
                   (onError [this t]    (info t :onError lease-id))
                   (onCompleted [this]  (info :onCompleted lease-id)))]
    (-> c lease-client
        (.keepAlive lease-id observer))))

(defn ^Lock lock-client
  "Gets a lock client from a client."
  [^Client c]
  (.getLockClient c))

(defn acquire-lock!
  "Acquires a lock with the given name and lease ID."
  [c name ^long lease-id]
  (-> c lock-client (.lock (->bytes name) lease-id) await ->clj))

(defn release-lock!
  "Releases a lock with the given lock ownership key."
  [c ^ByteSequence lock-key]
  (-> c lock-client (.unlock lock-key) await ->clj))

(defn ^Cluster cluster-client
  "Gets a cluster client for a client."
  [^Client client]
  (.getClusterClient client))

(defn list-members
  "Lists all members of a cluster."
  [client]
  (-> client cluster-client .listMember await ->clj))

(defn ^Maintenance maintenance-client
  "Gets a maintenance client for a client."
  [^Client client]
  (.getMaintenanceClient client))

(defn member-status
  "Returns the status of a particular node, identified by node name."
  [client node]
  (-> client
      maintenance-client
      (.statusMember (URI/create (support/peer-url node)))
      await
      ->clj))

(defn await-node-ready
  "Blocks until this node is responding to queries."
  [client]
  (-> client cluster-client .listMember (.get))
  true)
