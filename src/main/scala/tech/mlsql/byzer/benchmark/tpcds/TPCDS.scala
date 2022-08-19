package tech.mlsql.byzer.benchmark.tpcds

import org.apache.http.client.fluent.{Form, Request}
import tech.mlsql.byzer.benchmark.tpcds.BenchmarkPipelineApp.DirAndFormat
import tech.mlsql.byzer.benchmark.tpcds.TPCDS.tables
import tech.mlsql.common.Logging

import java.nio.charset.Charset
import java.time.LocalDateTime
import java.util.concurrent.{Callable, Executors}
import scala.collection.mutable.ArrayBuffer

class TPCDS(config: PipelineConfig, dir: DirAndFormat) extends Tpcds_2_4_Queries with Logging {
  private val arr = new ArrayBuffer[BenchmarkReport]()

  def addReport(benchmarkReport: BenchmarkReport): Unit = {
    synchronized( arr += benchmarkReport)
  }

  def getReport(): Seq[BenchmarkReport] = arr

  def createTables(): Unit = {
    val statements = tables.map( t => s"LOAD ${dir.format}.`${config.tpcdsDataDir}/${dir.subDir}/${t}` AS ${t};")
      .mkString(System.lineSeparator())

    runScriptSync(Query("loadTables", statements))
  }

  def runBenchmark(): Seq[BenchmarkReport] = {
    val threadPool = Executors.newFixedThreadPool(Math.max(1, 1))

    createTables()

    tpcds2_4Queries.foreach{ q =>
      threadPool.submit(new Callable[BenchmarkReport] {
        override def call(): BenchmarkReport = {
          // TODO If failureCountThreshold is reached, stop
          val report = runScriptSync(q)
          addReport(report)
          report
        }
      })
    }
    getReport()
  }

  def runScriptSync(query: Query): BenchmarkReport = {

    val request = Request.Get( config.engineUrl + "/run/script")
    request.setHeader("Content-Type", "application/x-www-form-urlencoded")
    // Assuming content does not contain multiple sql
    val sql = if( query.content.endsWith(";") ) query.content else query.content + ";"
    val form = Form.form()
    TPCDS.byzerParams( jobName = query.queryName ,
      sql = sql,
      defaultPathPrefix = config.defaultPathPrefix)
      .foreach( p => form.add(p._1, p._2))

    val startMillis = System.currentTimeMillis()
    val now = LocalDateTime.now().toString
    val httpResponse = request.bodyForm(form.build(), Charset.forName("utf-8")).execute().returnResponse()

    BenchmarkReport(query.queryName,
      httpResponse.getStatusLine.getStatusCode == 200,
      now,
      System.currentTimeMillis() - startMillis
    )
  }
}

object TPCDS {


  val baseParams: Map[String, String] = Map(
    "show_stack" -> "true",
    "context.__auth_client__" -> "streaming.dsl.auth.client.DefaultConsoleClient",
  )

  def byzerParams(jobName: String,
                  sql: String,
                  defaultPathPrefix: String,
                  timeout: Int = 9999999,
                  async: Boolean = false,
                  sessionPerUser: Boolean = true,
                  maxRetries: Int = 1,
                  owner: String = "admin"
                 ): Map[String, String] = {
    Map(
     "jobName" -> jobName,
     "sql" -> sql,
     "defaultPathPrefix" -> defaultPathPrefix,
     "timeout" -> timeout.toString,
      "async" -> async.toString,
      "sessionPerUser" -> sessionPerUser.toString,
      "maxRetries" -> maxRetries,
      "owner" -> owner
    ) + baseParams
  }
  val tables: Seq[String] = (
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

case class BenchmarkReport(query: String, succeed: Boolean, startTime: String, elapsedMilliseconds: Long)
