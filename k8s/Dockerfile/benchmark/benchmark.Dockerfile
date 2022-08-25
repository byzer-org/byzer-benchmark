FROM byzer/byzer-lang-k8s-aws:3.1.1-2.3.2-20220817
COPY run-benchmark.sh /work
ENTRYPOINT ["/work/run-benchmark.sh"]