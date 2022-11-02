# byzer-benchmark

## Running tpc-ds benchmark
1. edit run-benchmark.sh, replace classpath with your real path
2. run run-benchmark.sh , example:
```shell
./run-benchmark.sh --tpcdsDataDir s3a://byzer-bm/ \
--reportDir /tmp \
--engineUrl http://localhost:9004 \
--defaultPathPrefix /tmp/byzer-lang \
--failureCountThreshold 99 \
--useJuiceFS false \
--scaleFactor 1gb \
--format parquet
```

## Build benchmark image

1. Build the byzer-benchmark-0.0.1.jar
```shell
mvn package -DskipTests
```

2. Build the datagen image
Copy byzer-benchmark-0.0.1.jar to `k8s/datagen` directory; then build the image:

```shell
k8s/Dockerfile/datagen/build_benchmark_image.sh
```

3. Build the benchmark image
```shell
k8s/Dockerfile/benchmark/build_benchmark_image.sh
```