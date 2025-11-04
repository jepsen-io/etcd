FROM eclipse-temurin:21-alpine
# Install prereqs
RUN apk add git gnuplot graphviz grep
# Install test
WORKDIR /opt/jepsen-test
COPY jepsen-test.jar /opt/jepsen-test/jepsen-test.jar
# Go
ENTRYPOINT java -Djava.awt.headless=true -Xmx8g \
  -jar /opt/jepsen-test/jepsen-test.jar \
  test \
  --antithesis \
  --nodes n1,n2,n3 \
  --workload append \
  --concurrency 2n \
  --nemesis partition \
  --time-limit 60 \
  --serializable
