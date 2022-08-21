./run-benchmark.sh --tpcdsDataDir s3a://byzer-bm/ \
--reportDir /tmp \
--engineUrl http://localhost:9004 \
--defaultPathPrefix /tmp/byzer-lang \
--failureCountThreshold 99 \
--useJuiceFS false \
--scaleFactor 1gb \
--format parquet