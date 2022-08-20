package tech.mlsql.byzer.benchmark

import tech.mlsql.byzer.benchmark.tpcds.{BenchmarkPipelineApp, PipelineConfig}

object Test {
  def main(args: Array[String]): Unit = {
    val config = PipelineConfig(
      tpcdsDataDir = "s3a://zjc-2/",
      reportDir = "/work/tmp/",
      engineUrl = "http://192.168.50.254:9003",
      defaultPathPrefix = "/work/juicefs/byzer-lang-1/admin",
      failureCountThreshold = 1,
      useJuiceFS = "false",
      scaleFactor = "1gb",
      format = "parquet"
    )
    BenchmarkPipelineApp.run(config)
  }
}
