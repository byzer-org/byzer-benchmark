FROM seedjeffwan/spark:v2.4.5
LABEL maintainer="Jiachuan Zhu <chncaesar@gmail.com>"

# Copy tpcds-kit and benchmark library
COPY tpcds-kit/tools /opt/tpcds-kit/tools
COPY byzer-benchmark-0.0.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar /opt/spark/jars
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar /opt/spark/jars
ADD https://github.com/juicedata/juicefs/releases/download/v1.0.0/juicefs-hadoop-1.0.0.jar /opt/spark/jars

ENTRYPOINT [ "/opt/entrypoint.sh" ]