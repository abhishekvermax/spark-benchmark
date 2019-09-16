import java.io._

import BenchMarkReports.JsonReport
import SparkSubmitConfiguration.SparkSubmit.SparkSubmitParameters
import org.apache.spark.SparkContext
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import reports.Json.Report
import utils.ReportUtils._
import utils.TimePerBlockProvider.timeTaken
import operations.AlgebricOps._

object BenchMarkJob {


  val largeNumber: Int = 1024 * 1024

  def main(args: Array[String]) {
    val config = jobArgs(args).getOrElse(SparkSubmitParameters())
    val json = JsonReport("spark-benchmark")
    val sc = new SparkContext()

    val (mapTime, _, a) = timeTaken {
      val rdd = if (config.generate) {
        println("generated option")
        rddBlockSizeGenerator(sc, config)
      } else if (config.input.isDefined) {
        println("I/O option")
        createOutputDir(config.output.getOrElse("."), "/results")
        rddLargeMatrix(sc, config)
      } else {
        println("No option")
        sc.stop
        rddNoOptionFailureHandler(sc, config)
      }
      if (!config.lazyEval) {
        rdd.persist()
        val count = rdd.count() // firing action to get RDD lineage
        println(s"RDD count / generate = $count")
      }
      rdd
    }

    val (shiftTime, _, b) = timeTaken {
      val shiftResult = ShuffleRdd(a)
      if (!config.lazyEval) {
        shiftResult.persist()
        val count = shiftResult.count() // firing action to get RDD lineage
        println(s"RDD count / shift = $count")
      }
      shiftResult
    }

    val (avgTime, _, c) = timeTaken {
      val averageResult = doAverage(b, config)
      if (!config.lazyEval) {
        averageResult.persist()
        val count = averageResult.count() // firing action to get RDD lineage
        println(s"RDD count / average = $count")
      }
      averageResult
    }

    val (reduceTime, _, avgs) = timeTaken {
      val avgs = c.reduce(_ + _)
      println(s"avg(x)=${avgs(0)} avg(y)=${avgs(1)} avg(z)=${avgs(2)}")
      avgs
    }

    val timings = Map(
      "map" -> mapTime.t / 1.0e9,
      "shift" -> shiftTime.t / 1.0e9,
      "average" -> avgTime.t / 1.0e9,
      "reduce" -> reduceTime.t / 1.0e9,
      "overall" -> (mapTime.t + shiftTime.t + avgTime.t + reduceTime.t) / 1.0e9
    )

    val report = Report(timings)
    if (config.jsonFilename.isDefined)
      writeJsonReport(json, config, report)
  }

  def writeJsonReport(exp: JsonReport, config: SparkSubmitParameters, data: Report): Unit = {
    val results = exp.generateJson() ~ config.toJSON ~ data.toJSON
    val file = new File(config.jsonFilename.get)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(pretty(results))
    bw.close()
  }

  def nanoTime[R](block: => R): (Double, R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    (t1 - t0, result)
  }

  def jobArgs(args: Array[String]): Option[SparkSubmitParameters] = {
    val parser = new scopt.OptionParser[SparkSubmitParameters]("scopt") {
      head("spark-benchmark", "0.1.x")
      opt[String]('s', "input") action { (x, c) =>
        c.copy(input = Some(x))
      } text "input"
      opt[Unit]('g', "generated") action { (_, c) =>
        c.copy(generate = true)
      } text "g/generate is boolean"
      opt[Unit]('l', "lazy") action { (_, c) =>
        c.copy(lazyEval = true)
      } text "l/lazy if false turn-off caching"
      opt[String]('o', "output") action { (x, c) =>
        c.copy(output = Some(x))
      } text "output"
      opt[Int]('b', "blocks") action { (x, c) =>
        c.copy(blocks = x)
      } text "number of blocks"
      opt[Int]('m', "multiplier") action { (x, c) =>
        c.copy(multiplier = x)
      } text s"generate RDD test $largeNumber)"
      opt[Int]('k', "block_size") action { (x, c) =>
        c.copy(blockSize = x)
      } text s"s/blockSize is an int property (number of 3D float vectors x $largeNumber)"
      opt[Int]('n', "nodes") action { (x, c) =>
        c.copy(nodes = x)
      } text "n/nodes is an int property"
      opt[Int]('p', "nparts") action { (x, c) =>
        c.copy(nparts = x)
      } text "p/nparts is an int property"
      opt[Int]('c', "cores") action { (x, c) =>
        c.copy(cores = x)
      } text "c/cores is an int property (default to 12 for H-core processors)"
      opt[String]('j', "json") action { (x, c) =>
        c.copy(jsonFilename = Some(x))
      } text s"json <filename>is where to write JSON reports"

    }
    parser.parse(args, SparkSubmitParameters())
  }


}
