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
  --time-limit 120 \
  --nodes n1,n2,n3 \
  --concurrency 2n \
  --serializable
