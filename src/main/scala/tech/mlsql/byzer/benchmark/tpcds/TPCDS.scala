package tech.mlsql.byzer.benchmark.tpcds

import tech.mlsql.common.Logging

import scala.collection.mutable.ArrayBuffer

class TPCDS(config: PipelineConfig) extends Tpcds_2_4_Queries with Logging {
  private val arr = new ArrayBuffer[BenchmarkReport]()

  def addReport(benchmarkReport: BenchmarkReport): Unit = {
    synchronized( arr += benchmarkReport)
  }

  def addReports(reports: Seq[BenchmarkReport]): Unit = {
    synchronized( arr ++= reports)
  }

  def getReport(): Seq[BenchmarkReport] = arr



  def runBenchmark(): Seq[BenchmarkReport] = {

    val runQueries = if (config.queryFilter.nonEmpty) {
      val set = config.queryFilter.split(",").toSet
      tpcds2_4Queries.filter{ q => set.contains(q.queryName) }
    }
    else {
      tpcds2_4Queries
    }

    runQueries.foreach { q =>
      val report = ByzerHttpClient.runScriptSync(q, config)
      addReport(report)
      println(s"${report}")
    }

    getReport()
  }
}

object TPCDS {
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
