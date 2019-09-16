package utils

import squants.information.Bytes
import squants.time.Nanoseconds

object BlockTimeCalculatorSchema {

  case class TimeDiff(t: Double) {

    def +(next: TimeDiff): TimeDiff = TimeDiff(t + next.t)

    override def toString: String = s"Time(${Nanoseconds(t).toString})"

  }

  case class BytesDiff(m: Long) {
    val used: String = Bytes(m).toMegabytes.toString
    val free: String = Bytes(Runtime.getRuntime.freeMemory).toGibibytes.toString
    val total: String = Bytes(Runtime.getRuntime.totalMemory).toGibibytes.toString

    def +(next: BytesDiff): BytesDiff = BytesDiff(m + next.m)

    override def toString: String = s"Space(used=$used, free=$free, total=$total)"
  }

}
