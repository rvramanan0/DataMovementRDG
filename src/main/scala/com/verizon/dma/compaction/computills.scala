//import org.slf4j.LoggerFactory
//import scala.sys.process._
////import com.telstra.bidhadls.standardisestructure._
//import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
//import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, FileUtil}
////import com.telstra.bidhadls.compactor.config.CompactionSchemaConfiguration
//
//
//object computills {
//  lazy val logger = LoggerFactory.getLogger(getClass())
//
//  def ssuFolderName(ssu: String) : String = ssu match {
//    case "retail" => "retail"
//    case "wholesale" => "wholesale"
//    case "transient" => "transient"
//    case "enterprise" => "reference"
//  }
//
//  def createTableIfNotExists(tableName: String, colSpec: String, partitionSpec: Option[String], formatSpec: String, impalaHost: String) = {
//    val ddl = partitionSpec match {
//      case Some(partitionCols) => s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName}
//                                      (
//                                        ${colSpec.split(",").mkString(", \n\t")}
//                                      ) PARTITIONED BY ( ${partitionCols.split(",").mkString(", \n\t")} )
//                                      ${formatSpec}
//                                      ;
//                                  """
//      case None => s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName}
//                      (
//                        ${colSpec.split(",").mkString(", \n\t")}
//                      ) ${formatSpec}
//                      ;
//                    """
//    }
//
//    //logger.info(ddl)
//
//    val ddl_ret_code = ImpalaShell.executeStdIn(impalaHost, ddl)
//    require(ddl_ret_code == 0, "Impala table creation failed")
//  }
//
//  def createViewIfNotExists(viewDbName: String, viewName: String, databaseName: String, compactorOptions: CompactionSchemaConfiguration, impalaHost: String) = {
//    val cols = compactorOptions.writer.partitionSpec match {
//      case Some(x) => {
//        compactorOptions.writer.columnSpec.replace(", ", ",").split(",").map(x => x.split(" ").head) ++ x.replace(", ", ",").split(",").map(x => x.split(" ").head)
//      }.sorted.mkString(",")
//      case None => compactorOptions.writer.columnSpec.replace(", ", ",").split(",").map(x => x.split(" ").head).sorted.mkString(",")
//    }
//
//    val ddl = s"""CREATE VIEW IF NOT EXISTS ${viewDbName}.${viewName}
//                  AS
//                  SELECT
//                    ${compactorOptions.reader.columnSpec.replace(", ", ",").split(",").map(x => x.split(" ").head).sorted.mkString(",\n")}
//                  FROM ${databaseName}.${compactorOptions.reader.table}
//                  UNION
//                  SELECT
//                      ${cols}
//                  FROM ${databaseName}.${compactorOptions.writer.table}
//                  ;
//                """
//
//    //logger.info(ddl)
//    val ddl_ret_code = ImpalaShell.executeStdIn(impalaHost, ddl)
//    require(ddl_ret_code == 0, "Impala view creation failed")
//  }
//
//  def refreshDataInImpalaTable(tableName: String, impalaHost: String, refresh: Boolean= true, invalidate: Boolean = true, recover: Boolean = true) = {
//    // make newly inserted data available for
//    logger.info(s"Refreshing Metadata for table ${tableName}...")
//    if(refresh){
//      val ref_ret_code = ImpalaShell.executeStdIn(impalaHost, s"REFRESH ${tableName};")
//      require(ref_ret_code == 0, s"Refreshing Metadata for table ${tableName} failed")
//    }
//    if(invalidate){
//      val inv_ret_code = ImpalaShell.executeStdIn(impalaHost, s"INVALIDATE METADATA ${tableName};")
//      require(inv_ret_code == 0, s"Invalidating Metadata for table ${tableName} failed")
//    }
//    if(recover){
//      val ref_ret_code = ImpalaShell.executeStdIn(impalaHost, s"ALTER TABLE ${tableName} RECOVER PARTITIONS;")
//      require(ref_ret_code == 0, s"Recovering partitions for table ${tableName} failed")
//    }
//  }
//
//  def setPermissions(path: String)(implicit fs: FileSystem):Unit = {
//    val filePath = fs.makeQualified(new Path(path)).toString
//
//    if (fs.isFile(new Path(filePath))) { //filePath refers to a file
//      fs.setPermission(new Path(filePath),new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE , FsAction.READ_EXECUTE))
//    } else { //else filePath refers to a directory
//      fs.setPermission(new Path(filePath),new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE , FsAction.READ_EXECUTE))
//      val fileStatuses = fs.listStatus(new Path(filePath))
//      fileStatuses.foreach((f: FileStatus) => {
//        setPermissions(f.getPath.toString)
//      })
//    }
//  }
//
//
//}