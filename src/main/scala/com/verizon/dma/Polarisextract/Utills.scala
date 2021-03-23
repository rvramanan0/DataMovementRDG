package com.verizon.dma.Polarisextract

import org.apache.spark.sql.functions.{col, lit, sha2, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Utills {

  def stringToMap(extParams: String): Map[String, String] = {
    //    val param = extParams.replaceAll("^\"|\"$","")
    val param = extParams
    println(extParams)
    try {
      if (param.length != 0) {
        //Negative look back to handle the separator is used as value itself
        param.split("(?<!(=)),").map(x => {
          val k = x.split("(?<!(=))="); (k(0), k(1))
        }).toList.toMap
      } else Map[String, String]()
    } catch {
      case e: Exception =>
     //   logger.error("Unable to Convert String to Map : " + extParams + " to Map", e)
        throw e
    }
  }




  def generateorc(spark:SparkSession,dbname:String, tablename:String, filepath:String): Unit ={


    val hivetable = spark.read.option("multiline","true").json("C:\\Users\\Venkataramanan\\IdeaProjects\\DataMovementRDG\\data\\__default__\\example\\data\\hivetable.json")

    val hivetable1 = spark.sql("select * from dbname.tablename")


  }


  def joinDF(df1: DataFrame,  df2: DataFrame , joinExpr: Column, joinType: String, selectExpr: Seq[Column]): DataFrame = {
    val dfJoinResult = df1.join(df2, joinExpr, joinType)
    dfJoinResult.select(selectExpr:_*)
  }


//  val tableConfig = Map("a" -> "KEEP", "b" -> "HASH", "c" -> "KEEP", "d" -> "HASH", "e" -> "KEEP")
//
//  def hashColumns(tableConfig: Map[String, String], salt: String, df: DataFrame): DataFrame = {
//
//    val removedCols = tableConfig.filter(_._2 == "REMOVE").keys.toList
//    val hashedCols = tableConfig.filter(_._2 == "HASH").keys.toList
//
//    val cleanedDF = df.drop(removedCols: _ *)
//
//    val transformedCols = hashedCols.map{ c =>
//     // when(col(c).isNotNull , sha2(concat(col(c), lit(salt)), 256)).as(c)
//    }
//    val nonHashedCols = (cleanedDF.columns.toSet -- hashedCols.toSet).map(col(_)).toList
//    val hashedDF = cleanedDF.select((nonHashedCols ++ transformedCols):_*)
//
//    hashedDF
//  }

}
