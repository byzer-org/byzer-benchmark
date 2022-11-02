FROM byzer/byzer-lang-k8s-aws:3.1.1-2.3.2
LABEL maintainer="Jiachuan Zhu <chncaesar@gmail.com>"

# Copy tpcds-kit and benchmark library
RUN mkdir -p /home/deploy/byzer-benchmark/libs /opt/tpcds-kit/
COPY tpcds-kit /opt/tpcds-kit

#COPY postgresql-42.4.2.jar /home/deploy/byzer-lang/libs/

COPY byzer-benchmark-0.0.1.jar /home/deploy/byzer-benchmark/libs

ADD https://github.com/juicedata/juicefs/releases/download/v1.0.0/juicefs-hadoop-1.0.0.jar /home/deploy/byzer-lang/libs
RUN rm -f /home/deploy/byzer-lang/libs/juicefs-hadoop-0.17.5-linux-amd64.jar

#<!-- https://mvnrepository.com/artifact/org.apache.spark/spark-hadoop-cloud -->
#<dependency>
#    <groupId>org.apache.spark</groupId>
#    <artifactId>spark-hadoop-cloud_2.12</artifactId>
#    <version>3.1.1.3.1.7270.0-253</version>
#</dependency>
