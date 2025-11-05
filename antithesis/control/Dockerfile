FROM eclipse-temurin:21-alpine
# Install prereqs
RUN apk add git gnuplot graphviz grep
# Install test
WORKDIR /opt/jepsen-test
COPY jepsen-test.jar /opt/jepsen-test/jepsen-test.jar
# Antithesis instrumentation
RUN mkdir -p /opt/antithesis/catalog
# Instrumenting the JVM seems to cause some sort of JVM class/interface error
# in the pretty printer
#RUN ln -s /opt/jepsen-test /opt/antithesis/catalog
# Go
ENTRYPOINT java -Djava.awt.headless=true -Xmx8g \
  -ea \
  -cp '/opt/jepsen-test/*' \
  jepsen.etcd \
  #-jar /opt/jepsen-test/jepsen-test.jar \
  test \
  --nodes n1,n2,n3 \
  --workload append \
  --concurrency 2n \
  --nemesis partition \
  --time-limit 60 \
  --serializable
