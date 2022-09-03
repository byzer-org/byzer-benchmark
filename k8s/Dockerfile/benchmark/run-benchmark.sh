#!/usr/bin/env bash

java -XX:+HeapDumpOnOutOfMemoryError \
-Xmx4096m \
-XX:OnOutOfMemoryError="kill %p" \
-cp local:///home/deploy/byzer-benchmark/libs/byzer-benchmark-0.0.1.jar \
tech.mlsql.byzer.benchmark.tpcds.BenchmarkPipelineApp $@