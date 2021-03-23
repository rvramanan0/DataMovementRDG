package com.verizon.dma.Polarisextract

import com.verizon.dma.Polarisextract.Utills.stringToMap
import com.verizon.dma.Polarisextract.arrayexplode.splitt
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, lit, sha2, when}
import org.apache.spark.sql.{Column, SparkSession}
import sun.util.logging.resources.logging
import org.apache.spark.sql.functions.split

import java.util.Calendar

object test extends Logging {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    //step -1 Loading Property,Hash and hive table

    import spark.implicits._


    val proptable = spark.read.option("multiline", "true").json("C:\\Users\\Venkataramanan\\IdeaProjects\\DataMovementRDG\\data\\__default__\\example\\data\\property.json")
    val hivetable = spark.read.option("multiline","true").json("C:\\Users\\Venkataramanan\\IdeaProjects\\DataMovementRDG\\data\\__default__\\example\\data\\hivetable.json")

    //val proptable_maskcolumns = proptable.select($"columnstomask").map{x => x.mkString()}


    val proptable_maskcolumns = proptable.select("columnstomask").collect.mkString(",")

  //  val op = stringToMap(proptable_maskcolumns)

    println("helllo")
    println(proptable_maskcolumns)

//      collect().mkString.toList.map{x=>x.toString}
    logInfo("I am logmsg")

    val proptable_maskcolumns1 = proptable.select($"columnstomask")

    val selectExprs = 0 until 2 map (i => $"temp".getItem(i).as(s"col$i"))
    println("ddddd")
    val  kk = proptable.withColumn("temp", split($"columnstomask", ",")).select(selectExprs:_*).map(r =>r.toString()).collect.toList
//    val ignoremask = kk.map(x => x.getAs[String](0)).collect.toList
//    val ignoremasklist = (if(ignoremask.isEmpty) List("") else ignoremask.toList)

    println("ddddd")

//    y.show()
//    y.printSchema()
//    y.columns

    //val maskingcolumns = proptable_maskcolumns.map(x => x.getString(0)).mkString(",")
    // val maskingFieldsList = (if(maskingcolumns.isEmpty) List("") else maskingcolumns.toList)

    logInfo("I am logmsg")
    val excludedcolumns = hivetable.select(hivetable.columns .filter(colName => !proptable_maskcolumns.contains(colName)) .map(colName => new Column(colName)): _*)

    val excludedcolumns1 = proptable_maskcolumns1.select(proptable_maskcolumns1.columns .filter(colName => !proptable_maskcolumns.contains(colName)) .map(colName => new Column(colName)): _*)
//
//        val transformedCols = proptable_maskcolumns.map{ c =>
//          when(col(c.toString).isNotNull , sha2(col(c.toString), 256)).as(c)
//        }
//        val nonHashedCols = (cleanedDF.columns.toSet -- hashedCols.toSet).map(col(_)).toList
//        val hashedDF = cleanedDF.select((nonHashedCols ++ transformedCols):_*)


//    val removedCols = tableConfig.filter(_._2 == "REMOVE").keys.toList
//    val hashedCols = tableConfig.filter(_._2 == "HASH").keys.toList
//
//    val cleanedDF = df.drop(removedCols: _ *)

    println(proptable_maskcolumns)

val kk1 = kk.map{x => x.split(",").toString}

    println("mapped venat - "+kk1)
    val transformedCols = kk1.map{ c =>
      when(col(c).isNotNull , sha2(col(c.toString), 256)).as(c)
    }

    println(kk)
    println(transformedCols)
//  //  val nonHashedCols = (cleanedDF.columns.toSet -- hashedCols.toSet).map(col(_)).toList
 val hashedDF = hivetable.select((transformedCols):_*)
//
//
  hashedDF.printSchema()
   hashedDF.show()


    //    val mask = Seq("name", "age")
        val expr = hivetable.columns.map { col =>
          if (proptable_maskcolumns.contains(col)) s"""SHA2(${col},256) as ${col},"""
          else s"""${col},"""
        }

    val expr3 = hivetable.columns.map { col =>

      if (proptable_maskcolumns.contains(col)) s"""SHA2(${col},256) as ${col},"""
      else s"""${col},"""
    }



    logInfo("I am logmsg")

    val expr2 = expr.mkString. slice(0, expr.length - 1)

    logInfo("I am logmsg")

    val abc = Calendar.getInstance().getTime().toString

    //val hashedDF = hivetable.select((expr)


    //acctnoSHA2(custid,256) as custiddatedummyemailmsnnameSHA2(phno,256) as phno

    println(proptable_maskcolumns)
    println(excludedcolumns)
    excludedcolumns.printSchema()
    excludedcolumns.show()
    logInfo("I am logmsg")

    println(expr.mkString)
println(expr2.mkString)

    println(excludedcolumns1.toLocalIterator())


  }
}
