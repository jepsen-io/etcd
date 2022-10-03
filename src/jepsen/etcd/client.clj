(ns jepsen.etcd.client
  "Client library wrapper for jetcd"
  (:refer-clojure :exclude [await get swap!])
  (:require [clojure.core :as c]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.codec :as codec]
            [jepsen.etcd [support :as etcd.support]]
            [jepsen.etcd.client [etcdctl :as etcdctl]
                                [support :as s]
                                [txn :as t]]
            [jepsen.util :refer [coll]]
            [potemkin]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.lang AutoCloseable)
           (java.net URI)
           (java.nio.charset Charset)
           (java.util.function Consumer)
           (io.etcd.jetcd ByteSequence
                          Client
                          ClientBuilder
                          Cluster
                          KeyValue
                          KV
                          Lease
                          Lock
                          Maintenance
                          Response
                          Response$Header
                          Watch
                          Watch$Listener
                          Watch$Watcher)
           (io.etcd.jetcd.common.exception ClosedClientException
                                           CompactedException
                                           EtcdException)
           (io.etcd.jetcd.cluster Member
                                  MemberAddResponse
                                  MemberListResponse
                                  MemberRemoveResponse)
           (io.etcd.jetcd.kv CompactResponse
                             GetResponse
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
                             CmpTarget
                             Op
                             Op$PutOp
                             Op$GetOp)
           (io.etcd.jetcd.options CompactOption
                                  GetOption
                                  PutOption
                                  WatchOption)
           (io.etcd.jetcd.watch WatchEvent
                                WatchEvent$EventType
                                WatchResponse
                                WatchResponseWithError
                                WatchEvent$EventType)
           (io.grpc Status$Code
                    StatusRuntimeException)
           (io.grpc.stub StreamObserver)))

(def timeout
  "A default timeout, in ms."
  5000)

(def put-option-with-prev-kv
  "A put option which returns the previous kv"
  (.. (PutOption/newBuilder) withPrevKV build))

; Serialization

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


; Coercing responses to Clojure data
(defprotocol ToClj
  (->clj [o]))

(extend-protocol ToClj
  nil          (->clj [x] x)

  ByteSequence (->clj [bs] (bytes-> bs))

  CompactResponse (->clj [c]
                    {:header (->clj (.getHeader c))})

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

  MemberAddResponse (->clj [r]
                      {:header (->clj (.getHeader r))
                       :member (->clj (.getMember r))
                       :members (map ->clj (.getMembers r))})

  MemberListResponse (->clj [r]
                       {:header (->clj (.getHeader r))
                        :members (map ->clj (.getMembers r))})

  MemberRemoveResponse (->clj [r]
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

  WatchEvent (->clj [e]
               {:type     (->clj (.getEventType e))
                :kv       (->clj (.getKeyValue e))
                :prev-kv  (->clj (.getPrevKV e))})

  WatchEvent$EventType (->clj [e] (keyword (.toLowerCase (.name e))))

  WatchResponse (->clj [r]
                  {:header (->clj (.getHeader r))
                   :events (map ->clj (.getEvents r))})

  WatchResponseWithError (->clj [r]
                           {:response  (->clj (.getWatchResponse r))
                            :exception (.getException r)})
  )


; Opening and closing clients

(defn ^Client client
  "Builds a client for the given node. If given a test map, chooses what kind
  of client to create based on (:client-type test)."
  ([node]
   (.. (Client/builder)
       (endpoints (into-array String [(etcd.support/client-url node)]))
       ; (lazyInitialization false)
       ; (loadBalancerPolicy "some string???")
       (build)))
  ([test node]
   (case (:client-type test)
     :jetcd   (client node)
     :etcdctl (etcdctl/client test node))))

(defrecord EtcdCtl []
  )

(defn close!
  "Closes any client. Ignores ClosedClientExceptions."
  [^AutoCloseable c]
  (try
    (.close c)
    (catch ClosedClientException e
      :already-closed)))

(defmacro with-client
  "Opens a client for node, evaluates block with client-sym bound, closes
  client."
  [[client-sym node] & body]
  `(let [~client-sym (client ~node)]
    (try ~@body
         (finally (close! ~client-sym)))))

; Futures
(defn await
  "Derefences a future, using a default timeout, and throwing a slingshot
  exception of :type :timeout if it times out."
  [future]
  (let [r (deref future timeout ::timeout)]
    (if (= ::timeout r)
      (throw+ {:type      :timeout
               :definite? false})
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

(defn re-find+
  "Like re-find, but returns nil for nil strings instead of throwing. Jetcd
  likes to throw errors with missing message strings sometimes."
  [pattern string]
  (if (nil? string)
    nil
    (re-find pattern string)))

(defmacro remap-errors
  "Evaluates body, converting errors to thrown Slingshot maps with fields:

      :type         A keyword classifying the error
      :description  A descriptive object, e.g. a string
      :definite?    Is this error definitely a failure, or could it be ok?"
  [& body]
  `(try+ (unwrap-exceptions ~@body)
         (catch CompactedException e#
           (throw+ {:definite? true, :type :compacted, :description (.getMessage e#)}))

         (catch StatusRuntimeException e#
           (throw+
             (let [status# (.getStatus e#)
                   desc#   (.getDescription status#)]
               ; lmao, can't use a case statement here for ~reasons~
               (condp = (.getCode status#)
                 Status$Code/UNAVAILABLE
                 {:definite? false, :type :unavailable, :description desc#}

                 Status$Code/NOT_FOUND
                 {:definite? true, :type :not-found, :description desc#}

                 Status$Code/INVALID_ARGUMENT
                 (condp re-find+ desc#
                   #"duplicate key"
                   {:definite? true, :type :duplicate-key, :description desc#}
                   e#)

                 Status$Code/OUT_OF_RANGE
                 (condp re-find+ desc#
                   #"revision has been compacted"
                   {:definite? true, :type :revision-compacted, :description desc#}
                   e#)

                 Status$Code/UNKNOWN
                 (condp re-find+ desc#
                   #"leader changed"
                   {:definite? false, :type :leader-changed}

                   #"raft: stopped"
                   {:definite? true, :type :raft-stopped}

                   #"mutex: session is expired"
                   {:definite? false, :type :mutex-session-expired}

                   #"etcdserver: too many requests"
                   {:definite? true, :type, :etcdserver-too-many-requests}

                   (do (info "Unknown code=UNKNOWN description" (pr-str desc#))
                       e#))

                 ; Fall back to regular expressions on status messages
                 (do (info "Unknown error status code" (.getCode status#)
                           "-" status# "-" e#)
                     (condp re-find+ desc#
                       e#))))))

         (catch EtcdException e#
           (throw+
             (condp re-find+ (.getMessage e#)
               ; what even is this???
               #"Network closed for unknown reason"
               {:definite? false, :type :network-closed-unknown-reason}

               #"io exception"
               {:definite? false, :type :io-exception}

               #"unhealthy cluster"
               {:definite? true, :type :unhealthy-cluster}

               e#)))

         (catch java.net.ConnectException e#
           (throw+ {:definite?   true
                    :type        :connect-timeout
                    :description (.getMessage e#)}))

         (catch java.io.IOException e#
           (throw+
             (condp re-find+ (.getMessage e#)
               #"Connection reset by peer"
               {:definite? false, :type :connection-reset}

               e#)))

         (catch java.lang.IllegalStateException e#
           (throw+
             (condp re-find+ (.getMessage e#)
               ; Oooh, this one's rare
               #"call already half-closed"
               {:definite? false, :type :call-already-half-closed}

               ; Pretty sure this one's a bug in jetcd
               #"Stream is already completed"
               {:definite? false, :type :stream-already-completed}

               e#)))))

(defn client-error?
  "Returns true if this is a client error we know how to interpret. Useful as a
  try+ predicate."
  [m]
  (and (map? m)
       (contains? m :definite?)))

(defmacro with-errors
  "Takes an operation, a set of op types which are idempotent, and evals body,
  converting known exceptions to :fail or :info return ops."
  [op idempotent & body]
  `(try+ (remap-errors ~@body)
         (catch client-error? e#
           (assoc ~op
                  :type (if (or (:definite? e#)
                                (~idempotent (:f ~op)))
                          :fail
                          :info)
                  :error [(:type e#) (:description e#)]))))

(declare revision)

; KV ops

(defn ^KV kv-client
  "Extracts a KV client from a client."
  [^Client c]
  (.getKVClient c))

(defn compact!
  "Compacts history up to the given rev. Compacts physically--blocks until GC
  is complete. If no revision provided, compacts to the most recent revision."
  ([client]
   (compact! client (revision client)))
  ([client ^long rev]
   (info "Compacting to revision" rev)
   (-> client kv-client
       (.compact rev (.. (CompactOption/newBuilder)
                         (withCompactPhysical true)
                         build))
       await
       ->clj)))

(defn put!
  "Sets key to value, synchronously."
  [c k v]
  (-> c kv-client
      (.put (->bytes k) (->bytes v) put-option-with-prev-kv)
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
  "Evaluates a transaction on the given client. Takes a predicate, a true
  branch, and a false branch, as Clojure structures. See
  jepsen.etcd.client.txn for constructors."
  ([c t-branch]
   (txn! c nil t-branch))
  ([c test t-branch]
   (txn! c test t-branch nil))
  ([c test t-branch f-branch]
   (s/txn! c test t-branch f-branch)))

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

(defn member-ids->nodes
  "Looks up a map of member IDs to node names."
  [client]
  (->> (list-members client)
       :members
       (map (juxt :id :name))
       (into {})))

(defn member-id->node
  "Looks up a node name by member ID."
  [client member-id]
  (let [nodes (member-ids->nodes client)]
    (or (c/get nodes member-id)
        (throw+ {:type      ::no-such-node
                 :member-id member-id
                 :members   nodes}))))

(defn nodes->member-ids
  "Looks up a map of node IDS to member IDs."
  [client]
  (->> (list-members client)
       :members
       (map (juxt :name :id))
       (into {})))

(defn member-id
  "Looks up the member ID for a node. Throws if this node is not a member."
  [client node]
  (let [ids (nodes->member-ids client)]
    (or (c/get ids node)
        (throw+ {:type    ::no-such-member
                 :node    node
                 :members ids}))))

(defn add-member!
  "Adds one or more nodes to the cluster."
  [client node-or-nodes]
  (-> client
      cluster-client
      (.addMember (map #(URI/create (etcd.support/peer-url %)) (coll node-or-nodes)))
      await
      ->clj))

(defn remove-member-by-id!
  "Removes a single member from the cluster, by node ID."
  [client member-id]
  (-> client
      cluster-client
      (.removeMember member-id)
      await
      ->clj))

(defn remove-member!
  "Removes a single member from the cluster."
  [client node]
  (remove-member-by-id! client (member-id client node)))

(defn ^Maintenance maintenance-client
  "Gets a maintenance client for a client."
  [^Client client]
  (.getMaintenanceClient client))

(defn member-status
  "Returns the status of a particular node, identified by node name."
  [client node]
  (-> client
      maintenance-client
      (.statusMember (URI/create (etcd.support/peer-url node)))
      await
      ->clj))

(defn await-node-ready
  "Blocks until this node is responding to queries."
  [client]
  (or (try+ (remap-errors (-> client cluster-client .listMember (.get))
                          true)
            (catch client-error? e
              (warn e "Caught waiting for node to become ready")
              (Thread/sleep 1000)
              false))
      (recur client)))

(defn ^Watch watch-client
  "Gets a watch client from a client"
  [^Client client]
  (.getWatchClient client))

(defn watch-consumer
  "Builds a consumer for watches"
  [f]
  (reify Consumer
    (accept [_ event]
      (f (->clj event)))))

(defn ^Watch$Watcher watch
  "Watches key k, passing events to f. Use with-open or (.close) to close. If
  revision is provided, watches since that revision."
  ([client k f]
   (.watch (watch-client client)
           (->bytes k)
           (watch-consumer f)))
  ([client k revision f]
   (.watch (watch-client client)
           (->bytes k)
           (.. (WatchOption/newBuilder) (withRevision revision) build)
           (watch-consumer f)))
  ([client k revision f err-f complete-f]
   (.watch (watch-client client)
           (->bytes k)
           (.. (WatchOption/newBuilder) (withRevision revision) build)
           (watch-consumer f)
           (reify Consumer (accept [_ e] (err-f e)))
           complete-f)))

(defn revision
  "Returns the most current revision for this client."
  [c]
  (-> c (get* "") :header :revision))

(defn txn->java
  "Turns a transaction AST into an appropriate Java datatype. For instance,
  [:put \"foo\" \"bar\"] becomes an Op$PutOp."
  [x]
  (cond (sequential? x)
        (let [[type & args] x]
          (case type
            := (Cmp. (->bytes (first x)) Cmp$Op/EQUAL   (second x))
            :< (Cmp. (->bytes (first x)) Cmp$Op/LESS    (second x))
            :> (Cmp. (->bytes (first x)) Cmp$Op/GREATER (second x))
            :mod-revision     (CmpTarget/modRevision (first x))
            :create-revision  (CmpTarget/createRevision (first x))
            :value            (CmpTarget/value (->bytes (first x)))
            :version          (CmpTarget/version (first x))
            :get              (Op/get (->bytes (first x)) GetOption/DEFAULT)
            :put              (Op/put (->bytes (first x))
                                      (->bytes (second x))
                                      put-option-with-prev-kv)))))

(extend-protocol s/Client Client
  (txn! [c test t f]
    (let [test (coll test)
          t    (coll t)
          f    (coll f)
          res (-> (.. (kv-client c)
                      (txn)
                      (If   (into-array Cmp test))
                      (Then (into-array Op  t))
                      (Else (into-array Op  f))
                      (commit))
                  await
                  ->clj)
          ; Zip together get/put responses into a single sequence
          results (loop [rs   (transient [])
                         ops  (seq (if (:succeeded? res) t f))
                         gets (:gets res)
                         puts (:puts res)]
                    (if ops
                      (condp instance? (first ops)
                        Op$PutOp (recur (conj! rs (first puts))
                                        (next ops)
                                        gets
                                        (next puts))
                        Op$GetOp (recur (conj! rs (first gets))
                                        (next ops)
                                        (next gets)
                                        puts))
                      (persistent! rs)))]
      (assoc res :results results))))
