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
                           useJuiceFS: String = "true, false",
                           scaleFactor: String = "1gb,10gb,100gb",
                           format: String = "csv",
                           queryFilter: String = "",
                           awsAK: String = "",
                           awsSK: String = "",
                           sourceType: String = "file",  // file | postgresql
                           host: String = "",
                           userName: String = "",
                           pwd: String = "",
                           port: String = "",
                           database: String = "",
                           tableParallelism: String = ""
                         )

object BenchmarkPipelineApp {
  val bucketName = "byzer-bm"
  lazy val parser = new scopt.OptionParser[PipelineConfig]("tpcds-benchmark") {

    head("Byzer-lang TPC-DS benchmark")

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

    opt[String]('q', "queryFilter")
      .action {  (x, c) => c.copy(queryFilter = x)}
      .text("only runs query number")

    opt[String]( "sourceType")
      .action { (x, c) => c.copy(sourceType = x) }
      .text("sourceType: file or postgresql")
      .required()

    // FileDataSource related parameters
    opt[String]('d', "tpcdsDataDir")
      .action { (x, c) => c.copy(tpcdsDataDir = x)}
      .text("TPC-DS root data dir")

    opt[String]('j', "useJuiceFS")
      .action { (x, c) => c.copy(useJuiceFS = x)}
      .text("useJuiceFS: juciefs OR noJuicefs")

    opt[String]('s', "scaleFactor")
      .action { (x, c) => c.copy(scaleFactor = x)}
      .text("scaleFactor: 1gb OR 10gb OR 100gb OR 1tb")

    opt[String]('o', "format")
      .action { (x, c) => c.copy(format = x)}
      .text("format: csv OR parquet")

    opt[String]('a', "awsAK")
      .action { (x,c) => c.copy(awsAK = x)}
      .text("AWS access key")
      .required()

    opt[String]('k', "awsSK")
      .action { ( x, c) => c.copy(awsSK = x)}
      .text("AWS secret key")
      .required()

    // sourceType postgresql related paramters
    opt[String]("host")
      .action { (x, c) => c.copy(host = x)}
      .text("host")

    opt[String]("userName")
      .action { (x, c) => c.copy(userName = x)}
      .text("user name")

    opt[String]("pwd")
      .action { (x, c) => c.copy(pwd = x)}
      .text("password")

    opt[String]("database")
      .action { (x, c) => c.copy(database = x)}
      .text("password")

    opt[String]("port")
      .action { (x, c) => c.copy(port = x)}
      .text("port")

    opt[String]("tableParallelism")
      .action{ (x, c) => c.copy(tableParallelism = x) }
      .text("An array of table parallelism, format: tableName, partitionColumn, numPartitions, lowerBound, upperBound;" +
        " example: tbl1,id,1,1,50;tbl2,id,2,1,1000")
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, PipelineConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: PipelineConfig): Unit = {
    val loadReports = config.sourceType match {
      case "postgresql" => new PostgreSQLDataSource().loadTable(config)
      case "file" => new FileDataSource().loadTable(config)
    }

    val allReports = if( loadReports.forall(_.succeed)) {
      val benchmarkReports = new TPCDS(config).runBenchmark()
      loadReports ++ benchmarkReports
    }
    else {
      loadReports
    }

    saveReport(allReports, config)
  }

  def saveReport(reports: Seq[BenchmarkReport], config: PipelineConfig) : Unit = {
    val header = Array("query_name", "succeed", "start_time", "elapsed_milliseconds", "error")
    import scala.collection.JavaConverters._
    val nowStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val database =
      if ( config.sourceType == "file" ) s"tpcds_${config.format}_${config.useJuiceFS}_${config.scaleFactor}"
      else config.database

    val file = s"${config.reportDir}/${database}_${nowStr}.csv"
    val localFile = s"./${database}_${nowStr}.csv"
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
