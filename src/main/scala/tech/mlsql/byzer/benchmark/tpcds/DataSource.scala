package tech.mlsql.byzer.benchmark.tpcds

import tech.mlsql.byzer.benchmark.tpcds.TPCDS.tables

trait DataSource {
  def loadTable(config: PipelineConfig): Seq[BenchmarkReport]
}

case class TableParallelism(tableName: String, partitionColumn: String, numPartitions: Int = 1 , lowerBound: Long = 0, upperBound: Long = 0)

class PostgreSQLDataSource extends DataSource {
  override def loadTable(config: PipelineConfig): Seq[BenchmarkReport] = {
    val tableParallelisms =
      if( config.tableParallelism.nonEmpty) {
        config.tableParallelism.split(";").map { tp =>
          val arr = tp.split(",")
          TableParallelism(arr(0).trim, arr(1).trim, arr(2).trim.toInt, arr(3).trim.toLong, arr(4).trim.toLong)
        }.toSet
      }
      else {
        Set.empty
      }

    val (_, nonParalleled) = tables.partition( t => tableParallelisms.exists( tp => tp.tableName == t))

    val reports = nonParalleled.map { table =>
      val stmt =
        s"""LOAD jdbc.`${table}`
           |WHERE url = "jdbc:postgresql://${config.host}:${config.port}/${config.database}"
           |AND driver="org.postgresql.Driver"
           |AND user="${config.userName}"
           |AND password="${config.pwd}"
           |AS ${table};
           |""".stripMargin
      ByzerHttpClient.runScriptSync(Query(s"loadTable-${table}", stmt, Some(100)), config)
    }

    val parallelReports = tableParallelisms.map{ tp =>
      val stmt =
        s"""LOAD jdbc.`${tp.tableName}`
           |WHERE url = "jdbc:postgresql://${config.host}:${config.port}/${config.database}"
           |AND driver="org.postgresql.Driver"
           |AND user="${config.userName}"
           |AND password="${config.pwd}"
           |AND numPartitions="${tp.numPartitions}"
           |AND partitionColumn="${tp.partitionColumn}"
           |AND lowerBound="${tp.lowerBound}"
           |AND upperBound="${tp.upperBound}"
           |AS ${tp.tableName};
           |""".stripMargin

      ByzerHttpClient.runScriptSync(Query(s"loadTable-${tp.tableName}", stmt, Some(100)), config)
    }
    reports ++ parallelReports
  }
}

class FileDataSource extends DataSource {
  override def loadTable(config: PipelineConfig): Seq[BenchmarkReport] = {

    tables.map { table =>
      val csvConf = config.format match {
        case "csv" => """and `header`="true""".stripMargin
        case _ =>  " "
      }
      val database = s"tpcds_${config.format}_${config.useJuiceFS}_${config.scaleFactor}"
      val stmt = s"""load FS.`${config.tpcdsDataDir}/${database}/${table}`
                    |where `spark.hadoop.fs.AbstractFileSystem.s3a.impl`="org.apache.hadoop.fs.s3a.S3A"
                    |and `spark.hadoop.fs.s3a.endpoint`="s3.cn-northwest-1.amazonaws.com"
                    |and `spark.hadoop.fs.s3a.impl`="org.apache.hadoop.fs.s3a.S3AFileSystem"
                    |and `implClass`="${config.format}"
                    |${csvConf}
                    |AS ${table};
                    |""".stripMargin

      val report = ByzerHttpClient.runScriptSync(Query(s"loadTable-${table}", stmt, Some(10)), config)
      println( report )
      report
    }
  }
}
