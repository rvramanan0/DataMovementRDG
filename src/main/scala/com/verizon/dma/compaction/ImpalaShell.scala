//package com.verizon.dma.compaction
//
//import org.apache.spark.internal.Logging
//
//import java.io.ByteArrayInputStream
//import scala.sys.process._
//
//
//object ImpalaShell extends Logging {
//
////  **
////  * Execute a given execution string with impala
////  * @param server : the impala server to run against
////    * @param scriptContent : the script for impala to execute
////  * @return unix return code
////  */
//
//  def executeStdIn(server: String, scriptContent: String):Integer = {
//
//    val impalaShellProcess = scala.sys.process.Process(Seq("impala-shell","--kerberos","--ssl","-i",server,"-f","-"))
//    val infoLogger = scala.sys.process.ProcessLogger( o => logger.info(o) )
//    val scriptStream = new ByteArrayInputStream(scriptContent.getBytes("UTF-8"))
//
//    // Execute executionStr, returns return code
//    impalaShellProcess #< scriptStream ! infoLogger
//
//  }
//
//}
