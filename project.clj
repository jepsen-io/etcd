(defproject jepsen.etcd "0.2.5-SNAPSHOT"
  :description "etcd Jepsen test"
  :url "https://github.io/jepsen/etcd"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[jepsen "0.3.10-SNAPSHOT"
                  ; Antithesis needs a specific version and we're too new
                  :exclusions [com.fasterxml.jackson.core/jackson-core
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-databind]]
                 [io.jepsen/antithesis "0.1.0-SNAPSHOT"]
                 [tech.droit/clj-diff "1.0.1"]
                 [io.etcd/jetcd-core "0.8.5"
                  ; jetcd pulls in so, SO many incompatible deps with itself.
                  ; Every damn time I upgrade it takes 2 hours of untangling
                  ; their dependency web to get things working
                  :exclusions [io.netty/netty-codec-http2
                               io.netty/netty-handler-proxy
                               com.fasterxml.jackson.core/jackson-core
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-databind]]
                 [io.netty/netty-codec-http2 "4.1.118.Final"]
                 [io.netty/netty-handler-proxy "4.1.118.Final"]
                 ; We're stuck on this version because Antithesis only works
                 ; with Jackson ~2.2.3, and this version of Cheshire is close
                 ; enough to be compatible
                 [cheshire "5.3.1"
                  ; Antithesis needs a specific version and we're too new
                  :exclusions [com.fasterxml.jackson.core/jackson-core
                               com.fasterxml.jackson.core/jackson-annotations
                               com.fasterxml.jackson.core/jackson-databind]]
                 ]
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"
             "-Xmx72g"]
  :repl-options {:init-ns jepsen.etcd}
  :main jepsen.etcd
  :profiles {:uberjar {:target-path "target/uberjar"
                       :aot :all}})
