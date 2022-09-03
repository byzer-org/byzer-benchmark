package tech.mlsql.byzer.benchmark.tpcds

import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.util.EntityUtils
import tech.mlsql.byzer.benchmark.tpcds.BenchmarkPipelineApp.DirAndFormat
import tech.mlsql.byzer.benchmark.tpcds.TPCDS.tables
import tech.mlsql.common.Logging

import java.nio.charset.{Charset, StandardCharsets}
import java.time.LocalDateTime
import scala.collection.mutable.ArrayBuffer

class TPCDS(config: PipelineConfig, dir: DirAndFormat) extends Tpcds_2_4_Queries with Logging {
  private val arr = new ArrayBuffer[BenchmarkReport]()

  def addReport(benchmarkReport: BenchmarkReport): Unit = {
    synchronized( arr += benchmarkReport)
  }

  def addReports(reports: Seq[BenchmarkReport]): Unit = {
    synchronized( arr ++= reports)
  }

  def getReport(): Seq[BenchmarkReport] = arr

  def createTables(): Seq[BenchmarkReport] = {

    tables.map { t =>
      val csvConf = dir.format match {
        case "csv" => """and `header`="true""".stripMargin
        case _ =>  " "
      }
      val stmt = s"""load FS.`${config.tpcdsDataDir}/${dir.subDir}/${t}`
          |where `spark.hadoop.fs.AbstractFileSystem.s3a.impl`="org.apache.hadoop.fs.s3a.S3A"
          |and `spark.hadoop.fs.s3a.endpoint`="s3.cn-northwest-1.amazonaws.com"
          |and `spark.hadoop.fs.s3a.impl`="org.apache.hadoop.fs.s3a.S3AFileSystem"
          |and `implClass`="${dir.format}"
          |${csvConf}
          |AS ${t};
          |""".stripMargin

      val report = runScriptSync(Query(s"loadTable-${t}", stmt, Some(10)))
      println( report )
      report
    }

  }

  def runBenchmark(): Seq[BenchmarkReport] = {

    val reports = createTables()
    addReports(reports)
    if( reports.forall(_.succeed) ) {
      val runQueries = if (config.queryFilter.nonEmpty) {
        val set = config.queryFilter.split(",").toSet
        tpcds2_4Queries.filter{ q => set.contains(q.queryName) }
      }
      else {
        tpcds2_4Queries
      }

      runQueries.foreach { q =>
        // TODO If failureCountThreshold is reached, stop
        val report = runScriptSync(q)
        addReport(report)
        println(s"${report}")
      }
    }
    getReport()
  }

  def runScriptSync(query: Query): BenchmarkReport = {

    val request = Request.Post( config.engineUrl + "/run/script")
    request.setHeader("Content-Type", "application/x-www-form-urlencoded")
    // Assuming content does not contain multiple sql
    val sql = if( query.content != null && query.content.trim.nonEmpty && ! query.content.trim.endsWith(";") ) query.content + ";"
              else query.content
    println(sql)

    val form = Form.form()
    TPCDS.byzerParams( jobName = query.queryName ,
      sql = sql,
      defaultPathPrefix = config.defaultPathPrefix)
      .foreach( p => form.add(p._1, p._2))

    val startMillis = System.currentTimeMillis()
    val now = LocalDateTime.now().toString
    val httpResponse = request.bodyForm(form.build(), Charset.forName("utf-8")).execute().returnResponse()

    val err = httpResponse.getStatusLine.getStatusCode match {
      case 200 => Option.empty
      case _ =>
        val content = if (httpResponse.getEntity != null) EntityUtils.toByteArray(httpResponse.getEntity) else Array[Byte]()
        val errMsg = new String(content, StandardCharsets.UTF_8)
        println(errMsg)
        Some(errMsg)
    }

    val content = if (httpResponse.getEntity != null) EntityUtils.toByteArray(httpResponse.getEntity) else Array[Byte]()
    new String(content, StandardCharsets.UTF_8)
    BenchmarkReport(query.queryName,
      httpResponse.getStatusLine.getStatusCode == 200,
      now,
      System.currentTimeMillis() - startMillis,
      err
    )
  }
}

object TPCDS {
  val baseParams: Map[String, String] = Map(
    "show_stack" -> "true",
    "context.__auth_client__" -> "streaming.dsl.auth.client.DefaultConsoleClient",
    "fetchType" -> "take",
    "includeSchema" -> "false"
  )

  def byzerParams(jobName: String,
                  sql: String,
                  defaultPathPrefix: String,
                  timeout: Int = 9999999,
                  async: Boolean = false,
                  sessionPerUser: Boolean = true,
                  maxRetries: Int = 1,
                  owner: String = "admin",
                  outputSize: Option[Int] = Option.empty
                 ): Map[String, String] = {
    Map(
     "jobName" -> jobName,
     "sql" -> sql,
     "defaultPathPrefix" -> defaultPathPrefix,
     "timeout" -> timeout.toString,
      "async" -> async.toString,
      "sessionPerUser" -> sessionPerUser.toString,
      "maxRetries" -> maxRetries.toString,
      "owner" -> owner,
      "outputSize" -> outputSize.getOrElse(100).toString
    ) ++ baseParams
  }

  val tables: Seq[String] = Seq(
    "catalog_sales",
    "catalog_returns",
    "inventory",
    "store_sales",
    "store_returns",
    "web_sales",
    "web_returns",
    "call_center",
    "catalog_page",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "time_dim",
    "warehouse",
    "web_page",
    "web_site"
  )
}

case class BenchmarkReport(queryName: String, succeed: Boolean, startTime: String, elapsedMilliseconds: Long, errMsg: Option[String] = Option.empty)
