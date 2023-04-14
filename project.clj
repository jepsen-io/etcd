(defproject jepsen.etcd "0.2.3-SNAPSHOT"
  :description "etcd Jepsen test"
  :url "https://github.io/jepsen/etcd"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.2"
                  ; More jetcd conflicts
                  :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 [tech.droit/clj-diff "1.0.1"]
                 [io.etcd/jetcd-core "0.7.3"
                  ; jetcd pulls in so, SO many incompatible deps with itself
                  :exclusions [io.netty/netty-codec-http2
                               org.slf4j/slf4j-api
                               io.netty/netty-handler-proxy
                  ]]
                 [io.netty/netty-codec-http2 "4.1.78.Final"]
                 [io.netty/netty-handler-proxy "4.1.78.Final"]
                 [cheshire "5.11.0"]
                 ]
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"
             "-Xmx24g"]
  :repl-options {:init-ns jepsen.etcd}
  :main jepsen.etcd
  :profiles {:uberjar {:target-path "target/uberjar"
                       :aot :all}})
