//package com.verizon.dma.Polarisextract
//import scala.io.Source
//import java.nio.file.{Paths, Files}
//
//
//object pathconfig {
//
//  lazy val arguments = {
//    require(args.length == 1, "No arguments file specified")
//    val argumentPathName = args(0)
//    val argumentPath = Paths.get(argumentPathName)
//    require(Files.exists(argumentPath), s"Failed to find arguments file: $argumentPathName")
//
//    val jsonString = Source.fromFile(argumentPathName).mkString
//    Files.delete(argumentPath)
//    ArgumentsJsonParser.parse(jsonString)
//  }
//
//}
