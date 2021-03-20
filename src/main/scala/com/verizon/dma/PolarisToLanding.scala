package com.verizon.dma

import com.verizon.dma.beans.AppProperty
import org.apache.spark.sql.DataFrame
//import com.verizon.dma.compaction.computills.getColumnDefinitions
import org.apache.spark.sql.{Column, SparkSession, functions}
import org.apache.spark.sql.functions.{array, array_contains, col, lit, sha2, when}

object PolarisToLanding {

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

    val proptable = spark.read.option("multiline","true").json("C:\\Users\\Venkataramanan\\IdeaProjects\\DataMovementRDG\\data\\__default__\\example\\data\\property.json")
    val hivetable = spark.read.option("multiline","true").json("C:\\Users\\Venkataramanan\\IdeaProjects\\DataMovementRDG\\data\\__default__\\example\\data\\hivetable.json")
  val custhash = spark.read.option("multiline","true").json("C:\\Users\\Venkataramanan\\IdeaProjects\\DataMovementRDG\\data\\__default__\\example\\data\\custhash.json")
  val msntable = spark.read.option("multiline","true").json("data/__default__/example/data/gsamlogic.json")
    hivetable.printSchema();

//    val colsToRemove = Seq("colA", "colB", "colC", etc)
//    val filteredDF = df.select(df.columns .filter(colName => !colsToRemove.contains(colName)) .map(colName => new Column(colName)): _*)

    //    proptable.createTempView ("proptable")
//    hivetable.createTempView("hivetable")
//    custhash.createTempView("custhash")
//    msntable.createTempView("msntable")
//
//    val proptableop = spark.sql("select * from proptable")

    proptable.show()
    proptable.collect.foreach(println)
    custhash.show()

   // val maskingcolumns = proptable.select("columnstomask").rdd.map(x => x(0)).collect().toList

    import spark.implicits._


    val proptable_excludecolumns = proptable.select($"columnsexclude").collect()
    val excludecolumns = proptable_excludecolumns.map(x => x.get(0)).mkString(",")
    val ignoreFieldsList = (if(excludecolumns.isEmpty) List("") else excludecolumns.toList)


    val proptable_maskcolumns = proptable.select($"columnstomask").collect()
    val maskingcolumns = proptable_maskcolumns.map(x => x.get(0)).mkString(",")

    val liststring = proptable.select("columnstomask").map(r => r.getString(0)).collect.toList
    val maskingcolumns_char = maskingcolumns.toList


    val proptable_acctid = proptable.select($"acctid").collect()
    val acctid_flag = proptable_acctid.map(x => x.get(0)).mkString(",")

    val proptable_custid = proptable.select($"custid").collect()
    val custid_flag = proptable_custid.map(x => x.get(0)).mkString(",")

    val proptable_msn = proptable.select($"custid").collect()
    val msn_flag = proptable_msn.map(x => x.get(0)).mkString(",")

    val proptable_security = proptable.select($"custid").collect()
    val security_flag = proptable_security.map(x => x.get(0)).mkString(",")



    val hashArray = maskingcolumns.map(lit(_))

    println("hasharray-" +hashArray.toString())

    val salt = "abc"

//    var hivetable_mask = hivetable
//    for (col <- maskingcolumns){
//      hivetable_mask = hivetable_mask.withColumn(col, lit(sha2(col,256)))
//    }

//    val hashedDF = hivetable.columns.foldLeft(hivetable) {
//      (memoDF, colName) =>
//        memoDF.withColumn(
//          colName,
//
//          // 2nd.change: check if colName is in "a", "b", "c", "d" etc, if so apply sha2 otherwise leave the value as it is
//          when(col(colName).isNotNull && array_contains(array(hashArray:_*) ,
//            sha2(functions.concat(col(colName))
//          )
//        )
//    }





    println("printing hive table")
    hivetable.show()
  //  val fields = getColumnDefinitions(hivetable.schema)


    println("printing hash table")
   // hashedDF.printSchema()

   // val colsToRemove = Seq("colA", "colB", "colC", etc)


    val excludedcolumns = hivetable.select(hivetable.columns .filter(colName => !ignoreFieldsList.contains(colName)) .map(colName => new Column(colName)): _*)

//
//    def addColumnsViaFold(df: DataFrame, columns: List[String]): DataFrame = {
//      import df.sparkSession.implicits._
//      columns.foldLeft(df)((acc, col) => {
//        acc.withColumn(col, acc("incipit").as[String].contains(col))
//      })
//    }
//
//    val proptable_maskcolumns1 = proptable.select($"columnstomask")
//
//    println("inside function")
//
//    val addcoumns = addColumnsViaFold(hivetable,liststring)
//
//    addcoumns.printSchema()
//    addcoumns.show()
//    addcoumns.columns

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

    log.in

    testDf.printSchema()
    testDf.show()
    testDf.columns
//
//    val joinKeys="cust_id|cust_id"
//
//val joinkeyarray = joinKeys.toArray.mkString
//
//    val conditionArrays = joinkeyarray.split("\\|").map(c => c.split(","))
//
//
//    println(testDf)
//    val joinExpr = conditionArrays.map { case Array(a, b) => col("a." + a) === col("b." + b) }.reduce(_ and _)
//    val proptable_custid_df = proptable.select($"custid")
//
//    println(joinExpr)
//
//
//    val op =     hivetable.alias("a").join(proptable_custid_df.alias("b"), joinExpr, "left_outer")
//
//
//    op.printSchema()
//    op.show()
//    op.columns
//
//    println("end joinexp")

  //  val cond = fieldsToJoin.map(x => col(x._1) === col(x._2)).reduce(_ || _)

    //val newDF = maskingcolumns_char.foldLeft(excludedcolumns)((df, name) => df.withColumn(name,excludedcolumns.$"value"))



//      val hashcolumn = maskingcolumns.map(m => when(month(col("timestamp")) === m, col(s"${m}_col")))
//    //       change 7 to 8 to a sequence of all exsiting months columns




  //  hashedDF.show()

    println("leaving printing hash table2")


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


    println("proptablestring  - "+excludecolumns+"maskingcolumns-"  +maskingcolumns +"acctid_flag-"  +acctid_flag)
    println("msn_flag  - "+msn_flag +"custid_flag-"  +custid_flag+"security_flag-"  +security_flag)

//    if (col_val_str =="not")

//    println(hivetable.show())
//    println(custhash.show())
//    println(msntable.show())


//    df.registerTempTable("my_temp_table")
//    hc.sql("CREATE TABLE new_table_name STORED AS ORC  AS SELECT * from my_temp_table")

  }


}
