(ns jepsen.etcd.watch-test
  (:require [clojure.test :refer :all]
            [jepsen.etcd.watch :refer :all]
            [jepsen.util :refer [real-pmap]]
            [clojure.tools.logging :refer [info]]))

(info "hi")

(deftest converge-test
  (testing "basics"
    ; We're going to append random numbers to arrays until their final number is the same
    (let [n 3
          c (converger n (fn [colls] (apply = (map peek colls))))
          vs (real-pmap (fn [i]
                          (converge! c [i] (fn [coll]
                                             (Thread/sleep (rand-int 2))
                                             (conj coll (rand-int 2)))))
                        (range n))]
      ; Starts with initial values
      (is (= (range n) (map first vs)))
      ; Ends with same value
      (is (apply = (map peek vs)))))

  (testing "exceptions"
    (is (thrown-with-msg?
          RuntimeException #"^hi$"
          (let [n 3
                c (converger n (partial apply =))
                ; One worker throws a RuntimeException
                vs (real-pmap (fn [i]
                                (Thread/sleep (rand-int 10))
                                (if (= i 1)
                                  (throw (RuntimeException. "hi"))
                                  (rand-int 2)))
                              (range n))])))))
