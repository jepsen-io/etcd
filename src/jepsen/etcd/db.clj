(ns jepsen.etcd.db
  "Database setup and automation"
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.etcd [client :as client]
                         [support :as s]]
            [slingshot.slingshot :refer [try+]]))

(def dir "/opt/etcd")
(def binary "etcd")
(def logfile (str dir "/etcd.log"))
(def pidfile (str dir "/etcd.pid"))

(defn primary+term
  "Given a test and a node, returns a map of {:primary node :term term}, based
  on what this node thinks is the primary."
  [test node]
  (let [c (client/client node)]
    (try
      ; Build an index mapping node ids to node names
      (let [ids->nodes (->> (client/list-members c)
                            :members
                            (map (juxt :id :name))
                            (into {}))
            status (client/member-status c node)
            primary (-> status :leader ids->nodes)]
        {:primary primary
         :term    (:raft-term status)})
      (finally (client/close! c)))))

(defn primary
  "Picks the highest primary by term"
  [test]
  (->> (real-pmap (fn [node]
                    (try+ (primary+term test node)
                          (catch [:type :timeout] _ nil)))
                  (:nodes test))
       (remove nil?)
       (sort-by :term)
       last
       :primary))

(defn db
  "Etcd DB. Pulls version from test map's :version"
  []
  (reify
    db/Process
    (start! [_ test node]
      (c/su
        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary
          :--enable-v2
          :--log-outputs                  :stderr
          :--logger                       :zap
          :--name                         node
          :--listen-peer-urls             (s/peer-url   node)
          :--listen-client-urls           (s/client-url node)
          :--advertise-client-urls        (s/client-url node)
          :--initial-cluster-state        :new
          :--initial-advertise-peer-urls  (s/peer-url node)
          :--initial-cluster              (s/initial-cluster test))))

    (kill! [_ test node]
      (cu/stop-daemon! binary pidfile))

    db/Pause
    (pause!  [_ test node] (cu/grepkill! :stop "etcd"))
    (resume! [_ test node] (cu/grepkill! :cont "etcd"))

    db/Primary
    (setup-primary! [_ test node])

    (primaries [_ test]
      (list (primary test)))

    db/DB
    (setup! [db test node]
      (let [version (:version test)]
        (info node "installing etcd" version)
        (c/su
          (let [url (str "https://storage.googleapis.com/etcd/v" version
                         "/etcd-v" version "-linux-amd64.tar.gz")]
            (cu/install-archive! url dir))))
      (db/start! db test node)

      ; Wait for node to come up
      (let [c (client/client node)]
        (try
          (client/await-node-ready c)
          (finally (client/close! c)))))

    (teardown! [db test node]
      (info node "tearing down etcd")
      (db/kill! db test node)
      (c/su (c/exec :rm :-rf dir)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))
