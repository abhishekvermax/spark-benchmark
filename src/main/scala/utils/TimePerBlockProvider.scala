package utils

import utils.BlockTimeCalculatorSchema.{BytesDiff, TimeDiff}

object TimePerBlockProvider {

  def timeTaken[R](memBlock: => R): (TimeDiff, BytesDiff, R) = {
    val t0 = System.nanoTime()
    val m0 = Runtime.getRuntime.freeMemory

    val mBlock = memBlock

    val t1 = System.nanoTime()
    val m1 = Runtime.getRuntime.freeMemory
    val tAlpha = t1 - t0
    val tBeta = m0 - m1
    (TimeDiff(tAlpha), BytesDiff(tBeta), mBlock)
  }

}
