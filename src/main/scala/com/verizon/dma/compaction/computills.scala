//package com.verizon.dma.compaction
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
//import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
//import org.apache.hadoop.io.IOUtils
//import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
//import org.apache.spark.sql.functions.{col, first, lower, to_json}
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.slf4j.LoggerFactory
//
//import scala.util.Try
//
//object computills {
//
//  def getColumnDefinitions(schema: StructType) = {
//    schema.fields.map { x => x.name.toLowerCase -> (x.dataType match {
//      case StringType => "string"
//      case IntegerType => "int"
//      case LongType => "int"
//      case DoubleType => "double"
//      case DateType => "date"
//      case FloatType => "float"
//      case TimestampType => "timestamp"
//      case BooleanType => "boolean"
//      case ArrayType(DoubleType, true) => "array<double>"
//      case ArrayType(LongType, true) => "array<long>"
//      case ArrayType(FloatType, true) => "array<float>"
//      case ArrayType(IntegerType, true) => "array<int>"
//      case ArrayType(StringType, true) => "array<string>"
//      case x: DecimalType => s"decimal(${x.precision},${x.scale})"
//    }) }
//  }
//
//  private def createOrReplaceImpalaTable(df: DataFrame, databaseName: String, tableName: String, path: String,
//                                         partitionCols:List[String], impalaHost: String, ddlMode: String) = {
//    val fields = getColumnDefinitions(df.schema)
//    val cols = fields.filter(f => !partitionCols.contains(f._1))
//
//    val dropDdl = s"DROP TABLE IF EXISTS ${databaseName}.${tableName};\n"
//    val createDdl = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${databaseName}.${tableName} (\n\t" +
//      cols.map(f => { "`" + f._1 + "` " + f._2 }).mkString(", \n\t") +
//      s"\n) PARTITIONED BY (\n\t" +
//      partitionCols.map(f => s"${f} String").mkString(", \n\t") +
//      ")\nSTORED AS PARQUET\n" +
//      s"LOCATION '${path}';"
//
//    ddlMode.toLowerCase match {
//      case "create" => ImpalaShell.executeStdIn(impalaHost, createDdl)
//      case "drop_create" => ImpalaShell.executeStdIn(impalaHost, dropDdl + createDdl)
//      case _ => {
//        require(Set("CREATE", "DROP_CREATE").contains(ddlMode.toUpperCase),
//          "DDL Mode can be either create or drop_create")
//        1
//      }
//    }
//  }
//
//
//  private def createOrReplaceImpalaTable(df: DataFrame, databaseName: String, tableName: String, path: String,
//                                         partitionCols:List[String], impalaHost: String, ddlMode: String) = {
//    val fields = getColumnDefinitions(df.schema)
//    val cols = fields.filter(f => !partitionCols.contains(f._1))
//
//    val dropDdl = s"DROP TABLE IF EXISTS ${databaseName}.${tableName};\n"
//    val createDdl = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${databaseName}.${tableName} (\n\t" +
//      cols.map(f => { "`" + f._1 + "` " + f._2 }).mkString(", \n\t") +
//      s"\n) PARTITIONED BY (\n\t" +
//      partitionCols.map(f => s"${f} String").mkString(", \n\t") +
//      ")\nSTORED AS PARQUET\n" +
//      s"LOCATION '${path}';"
//
//    ddlMode.toLowerCase match {
//      case "create" => ImpalaShell.executeStdIn(impalaHost, createDdl)
//      case "drop_create" => ImpalaShell.executeStdIn(impalaHost, dropDdl + createDdl)
//      case _ => {
//        require(Set("CREATE", "DROP_CREATE").contains(ddlMode.toUpperCase),
//          "DDL Mode can be either create or drop_create")
//        1
//      }
//    }
//  }
//
//
//  def writeToImpala(df: DataFrame, databaseName: String, tableName: String, path: String,
//                    partitionCols: List[String], impalaHost: String, ddlMode: String = "drop_create") = {
//
//    val oldTimestampMode = df.sqlContext.getConf("spark.sql.parquet.int96AsTimestamp")
//    df.sqlContext.setConf("spark.sql.parquet.int96AsTimestamp","true")
//
//    df.repartition(partitionCols.map(col):_*)
//      .write
//      .partitionBy(partitionCols:_*)
//      .mode("append")
//      .format("parquet")
//      .save(path)
//
//    df.sqlContext.setConf("spark.sql.parquet.int96AsTimestamp", oldTimestampMode)
//
//    val ddl_ret_code = createOrReplaceImpalaTable(df, databaseName, tableName, path, partitionCols, impalaHost, ddlMode)
//    require(ddl_ret_code == 0, s"Impala DDL execution failed")
//  }
//
//  private def refreshImpalaMetadata(tableName: String, impalaHost: String) = {
//    val ref_ret_code = ImpalaShell.executeStdIn(impalaHost, s"REFRESH ${tableName};")
//    require(ref_ret_code == 0, s"Refreshing metadata for table ${tableName} failed")
//  }
//
//  private def recoverImpalaTable(tableName: String, impalaHost: String) = {
//    val ref_ret_code = ImpalaShell.executeStdIn(impalaHost, s"ALTER TABLE ${tableName} RECOVER PARTITIONS;")
//    require(ref_ret_code == 0, s"Recovering partitions for table ${tableName} failed")
//  }
//
//  private def invalidateImpalaMetadata(tableName: String, impalaHost: String) = {
//    val ref_ret_code = ImpalaShell.executeStdIn(impalaHost, s"INVALIDATE METADATA ${tableName};")
//    require(ref_ret_code == 0, s"Invalidating metadata for table ${tableName} failed")
//  }
//
//
//  def refreshImpala(tableName: String, impalaHost: String, refreshMode: String = "all") = {
//
//    refreshMode.toLowerCase match {
//      case "all" => {
//        recoverImpalaTable(tableName, impalaHost)
//        invalidateImpalaMetadata(tableName, impalaHost)
//        refreshImpalaMetadata(tableName, impalaHost)
//      }
//      case "recover" => recoverImpalaTable(tableName, impalaHost)
//      case "invalidate" => invalidateImpalaMetadata(tableName, impalaHost)
//      case "refresh" => refreshImpalaMetadata(tableName, impalaHost)
//    }
//  }
//
//}
