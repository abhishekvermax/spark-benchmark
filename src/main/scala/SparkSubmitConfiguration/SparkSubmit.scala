package SparkSubmitConfiguration

import org.json4s.JsonDSL._


object SparkSubmit {

  val largeNumber: Int = 1024 * 1024
  case class SparkSubmitParameters(
                                    input: Option[String] = None,
                                    output: Option[String] = None,
                                    cores: Int = 12,
                                    generate: Boolean = false,
                                    lazyEval: Boolean = false,
                                    blocks: Int = 1,
                                    blockSize: Int = 1, // 1 MB
                                    multiplier: Int = largeNumber,
                                    nparts: Int = 1,
                                    size: Int = 1,
                                    nodes: Int = 1,
                                    jsonFilename: Option[String] = None

                                  ) {

    def toJSON: org.json4s.JsonAST.JObject = {
      val properties = ("input" -> input.getOrElse("")) ~ ("output" -> output.getOrElse("")) ~ ("cores" -> cores.toString) ~
        ("generate" -> generate.toString) ~ ("lazy" -> lazyEval.toString) ~
        ("blocks" -> blocks.toString) ~ ("block_size" -> blockSize.toString) ~ ("multiplier" -> multiplier.toString) ~
        ("block_size_unit" -> "MB") ~
        ("nparts" -> nparts.toString) ~ ("size" -> size.toString) ~ ("nodes" -> nodes.toString) ~
        ("json" -> jsonFilename.getOrElse(""))

      "args" -> properties
    }

  }

}
