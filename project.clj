(defproject jepsen.etcd "0.2.2-SNAPSHOT"
  :description "etcd Jepsen test"
  :url "https://github.io/jepsen/etcd"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.2.7-SNAPSHOT"]
                 [tech.droit/clj-diff "1.0.1"]
                 [io.etcd/jetcd-core "0.7.1"
                  ; jetcd pulls in so, SO many incompatible deps
                  :exclusions [io.netty/netty-codec-http2
                               io.netty/netty-handler-proxy
                               ]]
                 [io.netty/netty-codec-http2 "4.1.74.Final"]
                 [io.netty/netty-handler-proxy "4.1.74.Final"]
                 ]
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"]
  :repl-options {:init-ns jepsen.etcd}
  :main jepsen.etcd)
