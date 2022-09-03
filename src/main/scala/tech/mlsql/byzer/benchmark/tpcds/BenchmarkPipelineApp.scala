package tech.mlsql.byzer.benchmark.tpcds

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import java.io.{File, FileWriter}
import com.opencsv.CSVWriter

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


case class PipelineConfig(
                           tpcdsDataDir: String = "",
                           reportDir: String = "",
                           engineUrl: String = "",
                           defaultPathPrefix: String = "",
                           failureCountThreshold: Int = 99,
                           useJuiceFS: String = "true, false, all",
                           scaleFactor: String = "1gb,10gb,100gb,1tb",
                           format: String = "csv",
                           queryFilter: String = "",
                           awsAK: String = "",
                           awsSK: String = ""
                         )

object BenchmarkPipelineApp {
  val bucketName = "byzer-bm"
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
      .text("useJuiceFS: juciefs OR noJuicefs")
      .required()

    opt[String]('s', "scaleFactor")
      .action { (x, c) => c.copy(scaleFactor = x)}
      .text("scaleFactor: 1gb OR 10gb OR 100gb OR 1tb")
      .required()

    opt[String]('o', "format")
      .action { (x, c) => c.copy(format = x)}
      .text("format: csv OR parquet")
      .required()

    opt[String]('q', "queryFilter")
      .action {  (x, c) => c.copy(queryFilter = x)}
      .text("only runs query number")

    opt[String]('a', "awsAK")
      .action { (x,c) => c.copy(awsAK = x)}
      .text("AWS access key")
      .required()

    opt[String]('k', "awsSK")
      .action { ( x, c) => c.copy(awsSK = x)}
      .text("AWS secret key")
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
    Seq( DirAndFormat(s"tpcds_${config.format}_${config.useJuiceFS}_${config.scaleFactor}", config.format ) )
  }

  def run(config: PipelineConfig): Unit = {
    // TODO Write values.yaml.[tpcdsDB] to values.yaml and reinstall helm chart. Note that helm should be installed
    // TODO Wait for byzer-lang to be available
    // launch a set of TPC-DS benchmarks sequentially
    generateSubDir(config).foreach { subDir =>
      println(s"tpcds dir: ${config.tpcdsDataDir}")

      val tpcds = new TPCDS(config, subDir)
      val reports = tpcds.runBenchmark()
      saveReport(reports, config, subDir)
    }
  }

  def saveReport(reports: Seq[BenchmarkReport], config: PipelineConfig, subDir: DirAndFormat) : Unit = {
    val header = Array("query_name", "succeed", "start_time", "elapsed_milliseconds", "error")
    import scala.collection.JavaConverters._
    val nowStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    val file = s"${config.reportDir}/${subDir.subDir}_${nowStr}.csv"
    val localFile = s"./${subDir.subDir}_${nowStr}.csv"
    tryWithResource(new CSVWriter(new FileWriter(localFile)) ) { w =>
      val data = reports.map { r =>
        val err = r.errMsg match {
          case Some(x) => x.substring(0, Math.min(x.length, 128))
          case _ => ""
        }
        Array( r.queryName, r.succeed.toString, r.startTime, r.elapsedMilliseconds.toString , err)
      }
      w.writeAll( (header +: data).asJava )
    }
    val credentials = new BasicAWSCredentials(config.awsAK, config.awsSK)
    val s3 = AmazonS3ClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.CN_NORTHWEST_1)
      .build()
    s3.putObject(bucketName, file, new File( localFile ) )
    println(s"Report saved as s3://${bucketName}/${file}")
  }

  def tryWithResource[A <: {def close(): Unit}, B](a: A)(f: A => B): B = {
    try f(a)
    finally {
      if (a != null) a.close()
    }
  }

}
