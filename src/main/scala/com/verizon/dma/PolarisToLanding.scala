package com.verizon.dma


import com.verizon.dma.beans.AppProperty
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.Iterator.empty.++
//import com.verizon.dma.compaction.computills.getColumnDefinitions
import org.apache.spark.sql.{Column, SparkSession, functions}
import org.apache.spark.sql.functions.{array, array_contains, col, lit, sha2, when}

object PolarisToLanding extends Logging {

//
//  if (System.getProperty("os.name").contains("Windows")) {
//    WinUtilsLoader.loadWinUtils()
//    buildSparkSession(AppProperty)
//  } else
//    createSparkSession(appName = appName)
//
//  startApp(arguments)
//}

  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    //step -1 Loading Property,Hash and hive table

    logInfo("Spark application started ")

   val proptable = spark.read.option("multiline","true").json("C:\\Users\\Venkataramanan\\IdeaProjects\\DataMovementRDG\\data\\__default__\\example\\data\\property.json")
    val hivetable = spark.read.option("multiline","true").json("C:\\Users\\Venkataramanan\\IdeaProjects\\DataMovementRDG\\data\\__default__\\example\\data\\hivetable.json")
   val custhash = spark.read.option("multiline","true").json("C:\\Users\\Venkataramanan\\IdeaProjects\\DataMovementRDG\\data\\__default__\\example\\data\\custhash.json")
  val msntable = spark.read.option("multiline","true").json("data/__default__/example/data/gsamlogic.json")
    hivetable.printSchema();



    proptable.show()
    proptable.collect.foreach(println)
    custhash.show()

   // val maskingcolumns = proptable.select("columnstomask").rdd.map(x => x(0)).collect().toList

    import spark.implicits._


    val proptable_excludecolumns = proptable.select($"columnsexclude").collect()
    val excludecolumns = proptable_excludecolumns.map(x => x.get(0)).mkString(",")
    val ignoreFieldsList = (if(excludecolumns.isEmpty) List("") else excludecolumns.toList)


    val proptable_maskcolumns = proptable.select($"columnstomask")
    //val maskingcolumns = proptable_maskcolumns.map(x => x.getString(0)).mkString(",")
   // val maskingFieldsList = (if(maskingcolumns.isEmpty) List("") else maskingcolumns.toList)






    //val maskingcolumnsList = (if(maskingcolumns.isEmpty) List("") else maskingcolumns.toList)

    val liststring = proptable.select("columnstomask").map(r => r.getString(0)).collect.toList
    //val maskingcolumns_char = maskingcolumns.toList


    val proptable_acctid = proptable.select($"acctid").collect()
    val acctid_flag = proptable_acctid.map(x => x.get(0)).mkString(",")

    val proptable_custid = proptable.select($"custid").collect()
    val custid_flag = proptable_custid.map(x => x.get(0)).mkString(",")

    val proptable_msn = proptable.select($"custid").collect()
    val msn_flag = proptable_msn.map(x => x.get(0)).mkString(",")

    val proptable_security = proptable.select($"custid").collect()
    val security_flag = proptable_security.map(x => x.get(0)).mkString(",")


    val tabletype = proptable.select($"tabletype").collect()
    val type_flag = proptable_security.map(x => x.get(0)).mkString(",")







//    var hivetable_mask = hivetable
//    for (col <- maskingcolumns){
//      hivetable_mask = hivetable_mask.withColumn(col, lit(sha2(col,256)))
//    }









    println("printing hive table")
    hivetable.show()
  //  val fields = getColumnDefinitions(hivetable.schema)


    println("printing hash table")
   // hashedDF.printSchema()

   // val colsToRemove = Seq("colA", "colB", "colC", etc)


    val excludedcolumns = hivetable.select(hivetable.columns .filter(colName => !ignoreFieldsList.contains(colName)) .map(colName => new Column(colName)): _*)

   // val excludedcolumns_mask = excludedcolumns.select(hivetable.columns .filter(colName => !maskingcolumnsList.contains(colName)) .map(colName => new Column(colName,256)): _*)

  //  val excludemaskingmap = stringToMap(maskingcolumns)

  //  excludedcolumns_mask.printSchema()

//    println("maskingvalues - "+maskingFieldsList)
//
//    val transformedCols = maskingFieldsList.map{ c =>
//      when(col(c.toString).isNotNull , sha2(col(c.toString),256))
//    }

//    val hashedDF = hivetable.select((transformedCols):_*)
//    println("hashvalue")
//    hashedDF.printSchema()
//    hashedDF.show()

    if(type_flag == "new") {
      val datefilter = excludedcolumns.filter($"date".between("2015-07-05", "2015-09-02"))
    }


//    else if((type_flag == "old")
//      {
//      val datefilter = excludedcolumns.filter($"date".between("2015-07-05", "2015-09-02"))
//    }
//    else if((type_flag == "adhoc")
//    {
//      val datefilter = excludedcolumns.filter($"date".between("2015-07-05", "2015-09-02"))
//    }
//
//    def addColumnsViaFold(df: DataFrame, columns: List[String]): DataFrame = {
//      import df.sparkSession.implicits._
//      columns.foldLeft(df)((acc, col) => {
//        acc.withColumn(col, acc("incipit").as[String].contains(col))
//      })
//    }
//


    println("outof function")

    println("insiode join cond")


    println("starting filter")

    def joinDF(df1: DataFrame,  df2: DataFrame , joinExpr: Column, joinType: String, selectExpr: Seq[Column]): DataFrame = {
      val dfJoinResult = df1.join(df2, joinExpr, joinType)
      dfJoinResult.select(selectExpr:_*)
    }
    val proptable_custid_df = proptable.select($"custid")

    val joinExpr = hivetable.col("custid") === proptable_custid_df.col("custid")
    //val selectExpr = hivetable.printSchema()
    println("starting filter join"+joinExpr)
    val selectExpr = hivetable.columns.map(hivetable(_)) ++ Array(proptable_custid_df("custid"))

    val testDf = joinDF(hivetable, proptable_custid_df, joinExpr, "inner", selectExpr)


//    val mask = Seq("name", "age")
//    val expr = hivetable.columns.map { col =>
//      if (maskingFieldsList.contains(col)) s"""SHA2(${col},256) as ${col}"""
//      else col
//    }



    println("maskingin2 - op")
   // hivetable.selectExpr(expr: _*).show

    println("maskingin2 - end")




    //
//    val transformedCols2 = transformedCols.select(transformedCols)


    //      val transformedCols = hashedCols.map{ c =>
    //        when(col(c).isNotNull , sha2(concat(col(c), lit(salt)), 256)).as(c)
    //      }


    testDf.printSchema()
    testDf.show()
    testDf.columns


    println("leaving printing hash table2")


    println("coming for security flag")


    //    GSAM security logic
if (security_flag == "Y") {

  println(security_flag)

  hivetable.createTempView("finalhivetableop")
  msntable.createTempView("gsamtable")


  val sqlquery = "select * from finalhivetableop a where not exists (select 1 from gsamtable b where a.msn = b.msn)"


  println(sqlquery)

  val finalset = spark.sql(sqlquery)
  finalset.show()

} else
  {


    println(security_flag)
    val finalset = testDf
    finalset.show()
  }





    println("outside for security flag")


    //    def hashColumns(tableConfig: Map[String, String], salt: String, df: DataFrame): DataFrame = {
//
//      val removedCols = tableConfig.filter(_._2 == "REMOVE").keys.toList
//      val hashedCols = tableConfig.filter(_._2 == "HASH").keys.toList
//
//      val cleanedDF = df.drop(removedCols: _ *)
//
//      val transformedCols = hashedCols.map{ c =>
//        when(col(c).isNotNull , sha2(concat(col(c), lit(salt)), 256)).as(c)
//      }
//      val nonHashedCols = (cleanedDF.columns.toSet -- hashedCols.toSet).map(col(_)).toList
//      val hashedDF = cleanedDF.select((nonHashedCols ++ transformedCols):_*)
//
//      hashedDF
//    }


    //Columns exclude

//    val cassnames5 = cassnames1.map {
//      x =>
//        val cass_table = x
//        val casscount = dfcount(x.toString)
//        (cass_table, casscount)
//    }


    println("proptablestring  - "+excludecolumns+"maskingcolumns-"   +"acctid_flag-"  +acctid_flag)
    println("msn_flag  - "+msn_flag +"custid_flag-"  +custid_flag+"security_flag-"  +security_flag)



//    df.registerTempTable("my_temp_table")
//    hc.sql("CREATE TABLE new_table_name STORED AS ORC  AS SELECT * from my_temp_table")

  }


}
