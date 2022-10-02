(ns jepsen.etcd.db
  "Database setup and automation"
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [lazyfs :as lazyfs]
                    [util :as util :refer [meh
                                           random-nonempty-subset]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.etcd [client :as client]
                         [support :as s]]
            [slingshot.slingshot :refer [throw+ try+]]))

(def dir "/opt/etcd")
(def binary "etcd")
(def logfile (str dir "/etcd.log"))
(def pidfile (str dir "/etcd.pid"))

(defn data-dir
  "Where does this node store its data on disk?"
  [node]
  (str dir "/" node ".etcd"))

(defn etcdctl!
  "Runs an etcdctl command on the local node."
  [& args]
  (c/su
    (c/exec (str dir "/etcdctl")
            :--endpoints (s/client-url (cn/local-ip)) args)))

(defn wipe!
  "Wipes data files on the current node."
  [test node]
  (c/su
    (c/exec :rm :-rf (str dir "/" node ".etcd")))
  ; We don't want these files coming back when lazyfs loses unsynced writes
  (when (:lazyfs test)
    (-> test :db :lazyfs lazyfs/checkpoint!)))

(defn from-highest-term
  "Takes a test and a function (f node client). Evaluates that function with a
  client bound to each node in parallel, and returns the response with the
  highest term."
  [test f]
  (let [rs (->> (:nodes test)
                (real-pmap (fn [node]
                             (try+
                               (client/remap-errors
                                 (client/with-client [c node] (f node c)))
                               (catch client/client-error? e nil))))
                (remove nil?))]
    (if (seq rs)
      (last (sort-by (comp :raft-term :header) rs))
      (throw+ {:type :no-node-responded}))))

(defn primary
  "Picks the highest primary by term"
  [test]
  (from-highest-term test
                     (fn [node c]
                       (->> (client/member-status c node)
                            :leader
                            (client/member-id->node c)))))

(defn initial-cluster
  "Takes a set of nodes and constructs an initial cluster string, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [nodes]
  (->> nodes
       (map (fn [node]
              (str node "=" (s/peer-url node))))
       (str/join ",")))

(defn start!
  "Starts etcd on the given node. Options:

    :initial-cluster-state    Either :new or :existing
    :nodes                    A set of nodes that will comprise the cluster."
  [test node opts]
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
      :--initial-cluster-state        (:initial-cluster-state opts
                                                              :existing)
      :--initial-advertise-peer-urls  (s/peer-url node)
      :--initial-cluster              (initial-cluster (:nodes opts))
      :--snapshot-count               (:snapshot-count test)
      (when (:unsafe-no-fsync test) :--unsafe-no-fsync)
      (when (:corrupt-check test)
        [:--experimental-initial-corrupt-check
         :--experimental-corrupt-check-time    "1m"])
      )))

(defn kill!
  "Kills etcd."
  []
  (c/su (cu/stop-daemon! binary pidfile)))

(defn members
  "Takes a test, asks all nodes for their membership, and returns the highest
  membership based on term."
  [test]
  (->> (from-highest-term test (fn [node client] (client/list-members client)))
       :members))

(defn refresh-members!
  "Takes a test and updates the current cluster membership, based on querying
  nodes presently in the test's cluster."
  [test]
  (let [raw-members (members test)
        members (->> raw-members
                     (map :name)
                     set)]
    (if (some str/blank? members)
      (throw+ {:type    ::blank-member-name
               :members raw-members})
      (do (info "Current membership is" (pr-str members))
          (reset! (:members test) members)))))

(defn addable-nodes
  "What nodes could we add to this cluster?"
  [test]
  (remove @(:members test) (:nodes test)))

(defn grow!
  "Adds a random node from the test to the cluster, if possible. Refreshes
  membership."
  [test]
  ; First, get a picture of who the nodes THINK is in the cluster
  (refresh-members! test)

  ; Can we add a node?
  (if-let [addable-nodes (seq (addable-nodes test))]
    (let [new-node (rand-nth addable-nodes)]
      (info :adding new-node)

      ; Tell the cluster the new node is a part of it
      (client/remap-errors
        (client/with-client [c (rand-nth (vec @(:members test)))]
          (client/add-member! c new-node)))

      ; Update the test map to include the new node
      (swap! (:members test) conj new-node)

      ; And start the new node--it'll pick up the current members from the test
      (c/on-nodes test [new-node]
                  (fn [test node]
                    (info :start! (class (:db test)) (pr-str (:db test)))
                    (db/start! (:db test) test node)))

      new-node)

    :no-nodes-available-to-add))

(defn shrink!
  "Removes a random node from the cluster, if possible. Refreshes membership."
  [test]
  ; First, get a picture of who the nodes THINK is in the cluster
  (refresh-members! test)
  ; Next, remove a node.
  (if (< (count @(:members test)) 2)
    :too-few-members-to-shrink

    (let [node (rand-nth (vec @(:members test)))]
      ; Ask cluster to remove it
      (let [contact (-> test :members deref (disj node) vec rand-nth)]
        (client/remap-errors
          (client/with-client [c contact]
            (info :removing node :via contact)
            (client/remove-member! c node))))

      ; Kill the node and wipe its data dir; otherwise we'll break the cluster
      ; when it restarts
      (c/on-nodes test [node]
                  (fn [test node]
                    (db/kill! (:db test) test node)
                    (info "Wiping" node)
                    (wipe! test node)))

      ; Record that the node's gone
      (swap! (:members test) disj node)
      node)))

(defrecord DB [tcpdump lazyfs]
  db/DB
  (setup! [db test node]
    (when (:tcpdump test)
      (db/setup! tcpdump test node))

    ; Install
    (let [version (:version test)]
      (info node "installing etcd" version)
      (c/su
        (let [url (str "https://storage.googleapis.com/etcd/v" version
                       "/etcd-v" version "-linux-amd64.tar.gz")]
          (cu/install-archive! url dir))))

    (when (:lazyfs test)
      (db/setup! (lazyfs node) test node))

    (db/start! db test node)

    ; Wait for node to come up
    (let [c (client/client node)]
      (try
        (client/await-node-ready c)
        (finally (client/close! c))))

    ; Once everyone's done their initial startup, we set initialized? to
    ; true, so future runs use --initial-cluster-state existing.
    (jepsen/synchronize test)
    (reset! (:initialized? test) true)

    (when (:lazyfs test)
      (lazyfs/checkpoint! (lazyfs node))))

  (teardown! [db test node]
    (when (:lazyfs test)
      (db/teardown! (lazyfs node) test node))
    (info node "tearing down etcd")
    (kill!)
    (c/su (c/exec :rm :-rf dir))
    (when (:tcpdump test)
      (db/teardown! tcpdump test node)))

  db/LogFiles
  (log-files [_ test node]
    ; hack hack hack
    (meh (c/su (c/cd dir
                     (c/exec :tar :cjf "data.tar.bz2" (data-dir node)))))
    (merge {logfile                   "etcd.log"
            (str dir "/data.tar.bz2") "data.tar.bz2"}
           (when (:tcpdump test) (db/log-files tcpdump test node))
           (when (:lazyfs test)  (db/log-files (lazyfs node) test node))))

  db/Primary
  (setup-primary! [_ test node])

  (primaries [_ test]
    (try+
      (list (primary test))
      (catch [:type :no-node-responded] e
        [])
      (catch [:type :jepsen.etcd.client/no-such-node] e
        (warn e "Weird cluster state: unknown node ID, can't figure out what primary is right now")
        [])))

  db/Process
  (start! [_ test node]
    (start! test node
            {:initial-cluster-state (if @(:initialized? test)
                                      :existing
                                      :new)
             :nodes                 @(:members test)}))

  (kill! [_ test node]
    (kill!)
    (when (:lazyfs test)
      (lazyfs/lose-unfsynced-writes! (lazyfs node))))

  db/Pause
  (pause!  [_ test node] (c/su (cu/grepkill! :stop "etcd")))
  (resume! [_ test node] (c/su (cu/grepkill! :cont "etcd"))))

(defn db
  "Etcd DB. Pulls version from test map's :version. Takes parsed CLI options."
  [opts]
  (map->DB {:tcpdump (db/tcpdump {:clients-only? true
                                  :ports [2379]})
            ; A map of nodes to the lazyfs DB for that node. We do this because
            ; each node names its data directory differently.
            :lazyfs  (->> (:nodes opts)
                          (map (fn [node]
                                 [node (lazyfs/db {:dir (data-dir node)
                                                   :cache-size "2GB"})]))
                          (into (sorted-map)))}))
