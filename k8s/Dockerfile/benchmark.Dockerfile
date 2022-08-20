FROM byzer/byzer-lang-k8s-aws:3.1.1-2.3.2
LABEL maintainer="Jiachuan Zhu <chncaesar@gmail.com>"

# Copy tpcds-kit and benchmark library
RUN mkdir -p /home/deploy/byzer-benchmark/libs /opt/tpcds-kit/
COPY tpcds-kit /opt/tpcds-kit

COPY byzer-benchmark-0.0.1.jar /home/deploy/byzer-benchmark/libs

ADD juicefs-hadoop-1.0.0.jar /home/deploy/byzer-lang/libs
COPY mlsql-mllib-3.0_2.12-0.1.0-SNAPSHOT.jar /home/deploy/byzer-lang/plugin/
RUN rm -f /home/deploy/byzer-lang/libs/juicefs-hadoop-0.17.5-linux-amd64.jar

ENTRYPOINT [ "/opt/entrypoint.sh" ]