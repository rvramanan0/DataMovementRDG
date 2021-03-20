package com.verizon.dma.sparksession


import java.io.File

/**
 * Singleton for loading winutils to application, for Windows local development
 */
object winutills {

  /**
   * Hadoop functionalities of Spark need the winutils.exe file to be present on Windows
   * http://letstalkspark.blogspot.fr/2016/02/getting-started-with-spark-on-window-64.html
   */
  def loadWinUtils(): Unit = {
    val sep = File.separator
    val current = new File(".").getAbsolutePath.replace(s"$sep.", "")
    System.setProperty("hadoop.home.dir", s"$current${sep}src${sep}main${sep}resources$sep")
  }


}

