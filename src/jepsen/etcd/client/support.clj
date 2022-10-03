(ns jepsen.etcd.client.support
  "Basic functions for working with clients.")

(defprotocol Client
  (txn! [client pred t-branch false-branch]
        "Takes a predicate test, an optional true branch, and an optional false branch. See jepsen.etcd.client.txn for how to construct these arguments."))
