package tech.mlsql.byzer.benchmark.tpcds

import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.util.EntityUtils

import java.nio.charset.{Charset, StandardCharsets}
import java.time.LocalDateTime

object ByzerHttpClient {

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

  def runScriptSync(query: Query, config: PipelineConfig): BenchmarkReport = {

    val request = Request.Post( config.engineUrl + "/run/script")
    request.setHeader("Content-Type", "application/x-www-form-urlencoded")
    // Assuming content does not contain multiple sql
    val sql = if( query.content != null && query.content.trim.nonEmpty && ! query.content.trim.endsWith(";") ) query.content + ";"
    else query.content
    println(sql)

    val form = Form.form()
    byzerParams( jobName = query.queryName ,
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
