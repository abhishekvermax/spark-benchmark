import org.json4s.JsonDSL._

object BenchMarkReports {

  case class JsonReport(name: String) {

    def generateJson(): org.json4s.JsonAST.JObject = "experiment" -> ("id" -> name)
  }

}
