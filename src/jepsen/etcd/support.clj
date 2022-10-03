(ns jepsen.etcd.support
  (:require [clojure.string :as str]
            [jepsen [control :as c]]
            [jepsen.control [core :as c.core]
                            [net :as c.net]]))

(def dir "/opt/etcd")

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (c.net/ip node) ":" port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node 2380))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node 2379))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str node "=" (peer-url node))))
       (str/join ",")))

(defn etcdctl!
  "Runs an etcdctl command with the current control session, against the local
  node. Takes an optional :in argument, after which should come a stdin
  string."
  [& args]
  (let [[command [_ stdin]] (split-with (complement #{:in}) args)
        cmd (->> [(str dir "/etcdctl")
                  :--endpoints (client-url (c.net/local-ip))
                  command]
                 (map c/escape)
                 (str/join " "))
        action {:cmd cmd, :in stdin}]
    (c/su
      (-> action
          c/wrap-cd
          c/wrap-sudo
          c/wrap-trace
          c/ssh*
          c.core/throw-on-nonzero-exit
          c/just-stdout))))
