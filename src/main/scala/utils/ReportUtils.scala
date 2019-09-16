package utils

import java.io.File

import scala.util.Try

object ReportUtils {


  def createOutputDir(dst: String, resultsDirName: String): Boolean = {
    val outdir = new File(dst, resultsDirName)
    Try {
      outdir.mkdirs()
    } getOrElse (false)
  }

}
