package tech.mlsql.byzer.benchmark.tpcds

case class PipelineConfig(
                           tpcdsDataDir: String = "",
                           reportDir: String = "",
                           engineUrl: String = "",
                           defaultPathPrefix: String = "",
                           failureCountThreshold: Int = 99,
                           useJuiceFS: String = "true, false, all",
                           scaleFactor: String = "1gb,10gb,100gb,1tb",
                           format: String = "csv"
                         )

object BenchmarkPipelineApp {

  lazy val parser = new scopt.OptionParser[PipelineConfig]("tpcds-benchmark") {

    head("Byzer-lang TPC-DS benchmark")

    opt[String]('d', "tpcdsDataDir")
      .action { (x, c) => c.copy(tpcdsDataDir = x)}
      .text("TPC-DS root data dir")
      .required()

    opt[String]('r', "reportDir")
      .action { (x, c) => c.copy(reportDir = x) }
      .text("Benchmark report dir")
      .required()

    opt[String]('u', "engineUrl")
      .action { (x, c) => c.copy(engineUrl = x) }
      .text("Byzer-lang url")
      .required()

    opt[String]('p', "defaultPathPrefix")
      .action { (x, c) => c.copy(defaultPathPrefix = x) }
      .text("Byzer-lang's defaultPathPrefix")
      .required()

    opt[Int]('f', "failureCountThreshold")
      .action { (x, c) => c.copy(failureCountThreshold = x)}
      .text("Failure count threshold to stop benchmark")
      .required()

    opt[String]('j', "useJuiceFS")
      .action { (x, c) => c.copy(useJuiceFS = x)}
      .text("useJuiceFS: ture OR false OR all")
      .required()

    opt[String]('s', "scaleFactor")
      .action { (x, c) => c.copy(scaleFactor = x)}
      .text("scaleFactor: 1gb OR 10gb OR 100gb OR 1tb OR all")
      .required()

    opt[String]('o', "format")
      .action { (x, c) => c.copy(format = x)}
      .text("format: csv OR parquet OR all")
      .required()
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, PipelineConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  case class DirAndFormat(subDir: String, format: String)
  def generateSubDir(config: PipelineConfig): Seq[DirAndFormat] = {
    // TODO
    Seq( DirAndFormat("parquet_false_1gb", "parquet" ) )
  }

  def run(config: PipelineConfig): Unit = {
    // TODO Write values.yaml.[tpcdsDB] to values.yaml and reinstall helm chart. Note that helm should be installed
    // TODO Wait for byzer-lang to be available
    // launch a set of TPC-DS benchmarks sequentially
    generateSubDir(config).foreach { subDir =>
      val tpcds = new TPCDS(config, subDir)
      val reports = tpcds.runBenchmark()
      // Write reports as csv
    }
  }

}
