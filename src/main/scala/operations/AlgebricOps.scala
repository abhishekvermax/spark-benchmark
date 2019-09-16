package operations

import java.io.DataInputStream

import SparkSubmitConfiguration.SparkSubmit.SparkSubmitParameters
import breeze.linalg.{*, DenseMatrix, DenseVector, Transpose, sum}
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import utils.TimePerBlockProvider.timeTaken

import scala.util.Try

object AlgebricOps {

  type LargeMatrix = Array[DenseMatrix[Double]]

  def rddLargeMatrix(sc: SparkContext, config: SparkSubmitParameters): RDD[LargeMatrix] = {
    val rdd = sc.binaryFiles(config.input.get)
    rdd.map(binaryRow => parser(binaryRow))
  }

  def parser(binFileInfo: (String, PortableDataStream)): LargeMatrix = {
    val (_, bin) = binFileInfo
    val dis = bin.open()
    val iter = Iterator.continually(nextDoubleFromStream(dis))
    val scalaArray = iter.takeWhile(_.isDefined).map(_.get).toArray
    val dMatrix = new DenseMatrix(scalaArray.length / 3, 3, scalaArray)
    Array(dMatrix)
  }

  def nextDoubleFromStream(dis: DataInputStream): Option[Double] = {
    Try {
      dis.readDouble
    }.toOption
  }

  def rddNoOptionFailureHandler(sc: SparkContext, config: SparkSubmitParameters): RDD[LargeMatrix] = {
    rddBlockSizeGenerator(sc, SparkSubmitParameters())
  }

  def rddBlockSizeGenerator(sc: SparkContext, config: SparkSubmitParameters): RDD[LargeMatrix] = {
    val rdd = sc.parallelize(0 to config.blocks, config.nodes * config.cores * config.nparts)
    rdd.map(item => generate(item, config.blockSize, config.multiplier))
  }

  // This is the RDD lambda
  def generate(id: Int, blockSize: Int, blockSizeMultiplier: Int): LargeMatrix = {
    val (deltat, _, array) = timeTaken {
      Array.fill(blockSize)(generateBlock(id, blockSizeMultiplier))
    }
    val blockCount = blockSize * blockSizeMultiplier
    println(s"Array (block id=$id) of $blockCount float vectors, time = $deltat")
    array
  }

  def generateBlock(id: Int, blockSize: Int): DenseMatrix[Double] = {
    val randomDoubleGenerator = RandomNumbers(id, -1000, 1000)
    DenseMatrix.fill(blockSize, 3)(randomDoubleGenerator.next)
  }

  def ShuffleRdd(a: RDD[LargeMatrix]): RDD[LargeMatrix] = {
    val shuffledVector = DenseVector(25.25, -12.125, 6.333)
    a.map(x => vectorShuffler(x, shuffledVector))
  }

  def doAverage(a: RDD[LargeMatrix], config: SparkSubmitParameters): RDD[DenseVector[Double]] = {
    a.map(x => averageOfVectorsUFunc(x))
  }

  def averageOfVectorsUFunc(data: LargeMatrix): DenseVector[Double] = {
    val (_, _, avg) = timeTaken {
      val partialAvgFunction = data map { m =>
        sum(m(::, *)).t.map { item => item / m.rows.toDouble }

      }
      partialAvgFunction.reduce(_ + _)
    }
    val finalAvg = avg.map { item => item / data.length.toDouble }
    finalAvg
  }


  def vectorShuffler(data: LargeMatrix, shift: DenseVector[Double]): LargeMatrix = {
    require {
      data.length > 0 && data(0).cols == shift.length
    }
    data.foreach { matrix =>
      for (i <- 0 until matrix.rows)
        matrix(i, ::) :+= Transpose(shift)
    }
    data
  }

  case class RandomNumbers(id: Int, low: Double, high: Double) {
    val start: Long = System.nanoTime() / (id + 1)
    val generator = new scala.util.Random(start)

    def next(): Double = low + (high - low) * generator.nextDouble()
  }

}
