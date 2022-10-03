(ns jepsen.etcd.client.etcdctl
  "A partial implementation of a client which shells out to etcdctl on the
  given node. We encode values using pr-str to make things easier to debug,
  rather than bytes."
  (:require [clojure [edn :as edn]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [cheshire.core :as json]
            [jepsen [control :as c]
                    [store :as store]
                    [util :as util :refer [coll]]]
            [jepsen.etcd [support :as support]]
            [jepsen.etcd.client.support :as s]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util Base64)
           (java.io Writer)))

(def client-number
  "Just for internal logging: a unique number for each client created."
  (atom 0))

(def log-dir
  "Where should we write logs?"
  "etcdctl-logs")

(defn etcdctl!
  "Runs an etcdctl command over the given jepsen.control session, passing the
  given stdin and returning parsed JSON."
  [node session cmd in]
  (util/timeout
    5000 (throw+ {:type :etcdctl-timeout,
                  :definite? false})
    (try+ (let [res (c/with-session node session
                      (support/etcdctl!
                        [cmd
                         :-w :json
                         :--dial-timeout "1s"
                         :--command-timeout "5s"]
                        :in in))]
            (json/parse-string res true))
          ; Rewrite errors
          (catch [:type :jepsen.control/nonzero-exit, :exit 1] e
            (let [err (:err e)
                  ;_ (warn :caught err)
                  first-msg (first (str/split-lines (:err e)))]
              (if (re-find #"^\{" first-msg)
                ; Maybe JSON?
                (let [parsed (json/parse-string (:err e) true)
                      error  (:error parsed)]
                  ; They squirrel away the actual error message in an error
                  ; field--msg is often something useless like "retrying of
                  ; unary invoker failed"
                  ;(warn parsed)
                  (throw+
                    (condp re-find error
                      #"duplicate key"
                      {:definite? true, :type :duplicate-key, :description error}

                      #"error reading from server: EOF"
                      {:definite? false, :type :eof}

                      {:definite? false, :type :etcdtcl, :description error})))

                ; Not JSON
                (throw+ {:definite? false, :type :etcdctl, :description err})))))))

(defn parse-header
  "Interprets a header"
  [header]
  {:member-id  (:member_id header)
   :revision   (:revision header)
   :raft-term  (:raft_term header)})

(defn parse-kv
  "Interprets a KV response"
  [{:keys [key create_revision mod_revision version value]}]
  (clojure.lang.MapEntry.
    (-> (Base64/getDecoder)
            (.decode key)
            String.
            edn/read-string)
    {:value (-> (Base64/getDecoder)
                (.decode value)
                String.
                edn/read-string)
     :version   version
     :create-revision create_revision
     :mod-revision mod_revision}))

(defn parse-kvs
  "Interprets a series of KV responses"
  [kvs]
  (into {} (mapv parse-kv kvs)))

(defn parse-response
  "Interprets a JSON Response object."
  [response]
  (let [r  (:Response response)
        ks (keys r)
        _  (assert (= 1 (count ks))
                   (str "Unexpected multi-key response " (pr-str response)))
        k  (first ks)
        v  (get r k)]
    (case k
      :response_put   {:header (parse-header (:header v))}
      :response_range {:header (parse-header (:header v))
                       :count  (:count v)
                       ; :more?  (< (count (:vs v)) (:count v))
                       :kvs    (parse-kvs (:kvs v))})))

(defn parse-res
  "Massages a JSON response into the same form the client expects."
  [res]
  (let [{:keys [header succeeded responses]} res]
    {:succeeded? succeeded
     :header    (parse-header header)
     :responses (mapv parse-response responses)}))

(defn txn->text
  "Turns a transaction AST (a Clojure structure) representing a transaction
  into text for etcdctl. See
  https://chromium.googlesource.com/external/github.com/coreos/etcd/+/release-3.0/etcdctl/README.md#txn-options
  for the syntax here."
  [x]
  (cond ; Hope escaping is sufficient for our purposes; this is a quick hack
        (string? x)
        (pr-str x)

        ; Integers are encoded with double-quotes too? I guess?
        (integer? x)
        (str "\"" x "\"")

        (sequential? x)
        (let [[type & args] x]
          (case type
            :txn (let [[pred t-branch f-branch] args]
                   (str/join "\n"
                             (concat (map txn->text pred)
                                     [""]
                                     (map txn->text t-branch)
                                     [""]
                                     (map txn->text f-branch)
                                     ["\n"])))

            ; arrrrgh they encode the syntax tree two incompatible ways between
            ; jetcd and the etcdctl text version. Our AST is [:< k [:mod 5]],
            ; but we have to spit out mod(k) < 5. Always put the mod (or other
            ; fun) first.
            (:= :< :>)
            (let [[k target] args
                  [fun value] target]
              (str (case fun
                     :mod-revision "mod"
                     :value        "val"
                     :version      "ver")
                   "(" (txn->text k) ") " (name type) " " (txn->text value)))

            :put (str "put " (first args) " " (txn->text (pr-str (second args))))
            :get (str "get " (first args))))))

(defprotocol Log
  (log [this msg]))

(defrecord EtcdctlClient [number, ^Writer log, node, session]
  s/Client
  (txn!
    [this pred t-branch f-branch]
    (try
      (.write log "\n-------------------------------------------------\n")
      (let [txn  [:txn pred t-branch f-branch]
            text (txn->text txn)
            _    (.write log (str (util/local-time)))
            _    (.write log "\n")
            _    (.write log text)
            ;_   (info :txn txn "\n" (txn->text txn))
            raw-res (etcdctl! node session :txn text)
            _       (.write log (util/pprint-str raw-res))
            _       (.write log "\n")
            ;_    (info :raw-res (util/pprint-str raw-res))
            res (parse-res raw-res)
            _   (.write log (util/pprint-str res))
            ; Parse get/put results
            ;_    (info :parsed (util/pprint-str res))
            res (-> res
                    (dissoc :responses)
                    (assoc :results
                           (->> res
                                :responses)))]
        ;(info :res (util/pprint-str res))
        res)
      (catch Throwable t
        (.write log (str "Error: " t "\n\n"))
        (throw t))))

  Log
  (log [this msg]
    (.write log msg)
    (.write log "\n"))

  java.lang.AutoCloseable
  (close [this]
    (.flush log)
    (.close log)
    (c/disconnect session)))

(defn client
  "Constructs a client for the given test and node."
  [test node]
  (let [client-number (swap! client-number inc)]
    (EtcdctlClient. client-number
                    (io/writer (store/path! test log-dir
                                            (str client-number ".log")))
                    node
                    (c/session node))))
