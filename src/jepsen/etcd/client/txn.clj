(ns jepsen.etcd.client.txn
  "Support functions for constructing transactional expressions. These
  construct little Clojure ASTs like [:< [:mod-revison \"foo\" 5] 6]."
  (:refer-clojure :exclude [get < = > compare]))

(defn get
  "Constructs a get operation."
  [k]
  [:get k])

(defn put
  "Constructs a put operation."
  [k v]
  [:put k v])

(defn version
  "A comparison target by version."
  [v]
  [:version v])

(defn value
  "A comparison target by value."
  [v]
  [:value v])

(defn mod-revision
  "A comparison target by modification revision."
  [revision]
  [:mod-revision revision])

(defn create-revision
  "A comparison target by creation revision."
  [revision]
  [:create-revision revision])

(defn =
  "Constructs an equality comparison between key and target."
  [k target]
  [:= k target])

(defn <
  "Constructs an LESS inequality comparison between key and target."
  [k target]
  [:< k target])

(defn >
  "Constructs an GREATER inequality comparison between key and target."
  [k target]
  [:> k target])
