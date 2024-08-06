/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package benchmark

import benchmark.TPCDSBenchmarkQueries._

trait TPCDSConf extends BenchmarkConf {
  protected def format: Option[String]
  def scaleInGB: Int
  def userDefinedDbName: Option[String]

  def formatName: String = format.getOrElse {
    throw new IllegalArgumentException("format must be specified")
  }
  def dbName: String = userDefinedDbName.getOrElse(s"tpcds_sf${scaleInGB}_${formatName}")
  def dbLocation: String = dbLocation(dbName)
}

case class TPCDSBenchmarkConf(
     protected val format: Option[String] = None,
     scaleInGB: Int = 0,
     userDefinedDbName: Option[String] = None,
     iterations: Int = 3,
     benchmarkPath: Option[String] = None,
     queryOffset: Int = 1,
     queryLimit: Int = 100000,
     skippedQueries: Array[Int] = Array.empty[Int],
     cherryPickedQueries: Array[Int] = Array.empty[Int]) extends TPCDSConf

object TPCDSBenchmarkConf {
  import scopt.OParser
  private val builder = OParser.builder[TPCDSBenchmarkConf]
  private val argParser = {
    import builder._
    OParser.sequence(
      programName("TPC-DS Benchmark"),
      opt[String]("format")
        .required()
        .action((x, c) => c.copy(format = Some(x)))
        .text("Spark's short name for the file format to use"),
      opt[String]("scale-in-gb")
        .required()
        .valueName("<scale of benchmark in GBs>")
        .action((x, c) => c.copy(scaleInGB = x.toInt))
        .text("Scale factor of the TPCDS benchmark"),
      opt[String]("benchmark-path")
        .required()
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(benchmarkPath = Some(x)))
        .text("Cloud path to be used for creating table and generating reports"),
      opt[String]("iterations")
        .optional()
        .valueName("<number of iterations>")
        .action((x, c) => c.copy(iterations = x.toInt))
        .text("Number of times to run the queries"),
      opt[String]("queryOffset")
        .optional()
        .valueName("<query offset>")
        .action((x, c) => c.copy(queryOffset = x.toInt))
        .text("Offset to start queries with"),
      opt[String]("queryLimit")
        .optional()
        .valueName("<query limit>")
        .action((x, c) => c.copy(queryLimit = x.toInt))
        .text("Limit till queries to be run"),
      opt[String]("skippedQueries")
        .optional()
        .valueName("<skipped queries>")
        .action((x, c) => c.copy(skippedQueries = x.split(",").map(_.toInt)))
        .text("Skipped Queries"),
      opt[String]("cherryPickedQueries")
        .optional()
        .valueName("<cherry picked queries>")
        .action((x, c) => c.copy(cherryPickedQueries = x.split(",").map(_.toInt)))
        .text("Cherry Picked Queries"),
    )
  }

  def parse(args: Array[String]): Option[TPCDSBenchmarkConf] = {
    OParser.parse(argParser, args, TPCDSBenchmarkConf())
  }
}

class TPCDSBenchmark(conf: TPCDSBenchmarkConf) extends Benchmark(conf) {
  val queries: Map[String, String] = {
    if (conf.scaleInGB <= 3000) TPCDSQueries3TB
    else if (conf.scaleInGB > 3000) TPCDSQueries10TB
    else throw new IllegalArgumentException(
      s"Unsupported scale factor of ${conf.scaleInGB} GB")
  }

  val dbName = conf.dbName
  val extraConfs: Map[String, String] = Map(
    "spark.sql.broadcastTimeout" -> "7200",
    "spark.sql.crossJoin.enabled" -> "true",

  )

  def runInternal(): Unit = {
    for ((k, v) <- extraConfs) spark.conf.set(k, v)
    spark.sparkContext.setLogLevel("WARN")
    log("All configs:\n\t" + spark.conf.getAll.toSeq.sortBy(_._1).mkString("\n\t"))
    spark.sql(s"USE hive_cat.tpcds_sf${conf.scaleInGB}_iceberg_gcs")
    val regex = """\b(\d+)\b""".r
    for (iteration <- 1 to conf.iterations) {
      queries.toSeq.sortBy(_._1).foreach { case (name, sql) => {
          val queryNum: Int = regex.findFirstMatchIn(name.trim.split("q").lift(1).getOrElse("0").split("a").lift(0).getOrElse("0").split("b").lift(0).getOrElse("0")).map(_.group(1).toInt).getOrElse(0)
          if(!conf.cherryPickedQueries.isEmpty) {
            if(conf.cherryPickedQueries.contains(queryNum)) {
//              log("Eligible for Running in cherryPicked flow: " + name)
              runQuery(sql, iteration = Some(iteration), queryName = name)
            } else {
              log("Skipping: " + name + " " + queryNum)
            }
          } else if(queryNum >= conf.queryOffset && queryNum <= (conf.queryOffset + conf.queryLimit) && !conf.skippedQueries.contains(queryNum)) {
//            log("Eligible for Running: " + name)
            runQuery(sql, iteration = Some(iteration), queryName = name)
          } else {
            log("Skipping: " + name + " " + queryNum)
          }
        }
      }
    }
    val results = getQueryResults().filter(_.name.startsWith("q"))
    if (results.forall(x => x.errorMsg.isEmpty && x.durationMs.nonEmpty) ) {
      val medianDurationSecPerQuery = results.groupBy(_.name).map { case (q, results) =>
        assert(results.size == conf.iterations)
        val medianMs = results.map(_.durationMs.get).sorted
            .drop(math.floor(conf.iterations / 2.0).toInt).head
        (q, medianMs / 1000.0)
      }
      val sumOfMedians = medianDurationSecPerQuery.map(_._2).sum
      reportExtraMetric("tpcds-result-seconds", sumOfMedians)
    }
  }
}

object TPCDSBenchmark {
  def main(args: Array[String]): Unit = {
    TPCDSBenchmarkConf.parse(args).foreach { conf =>
      new TPCDSBenchmark(conf).run()
    }
  }
}

