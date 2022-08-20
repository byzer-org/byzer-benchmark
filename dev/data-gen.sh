#!/usr/bin/env bash

spark-submit --master local \
--class tech.mlsql.byzer.benchmark.tpcds.DataGeneration \
--conf spark.driver.memory=4g \
--conf spark.driver.cores=1 \
--conf spark.executor.memory=4g \
--conf spark.executor.cores=1 \
--conf spark.executor.instances=1 \
--conf spark.executor.extraClassPath=/work/server/byzer-benchmark/libs/hadoop-aws-3.1.1.jar:/work/server/byzer-benchmark/libs/aws-java-sdk-bundle-1.11.271.jar \
--conf spark.driver.extraClassPath=/work/server/byzer-benchmark/libs/hadoop-aws-3.1.1.jar:/work/server/byzer-benchmark/libs/aws-java-sdk-bundle-1.11.271.jar \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
/work/server/byzer-benchmark/libs/byzer-benchmark-0.0.1.jar \
s3a://zjc-2/tpcds_parquet_nojuicefs_1gb \
/work/server/byzer-benchmark/tpcds-kit/ \
parquet \
1 \
10 \
false \
false \
false