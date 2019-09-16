package reports

import org.json4s.JsonDSL._
import org.json4s.{JField, JString}

object Json {

  case class Report(timings: Map[String, Double]) {
    def toJSON(): org.json4s.JsonAST.JObject = {
      val timeData = timings map { case (name, value) => JField(name, JString(value.toString)) }
      JField("performance", timeData.toList)
    }
  }




}
