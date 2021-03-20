package com.verizon.dma.sparksession



import com.verizon.dma.beans.AppProperty

import java.util.Properties
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Trait that provides the management of a SparkSession
 */
trait buildsession extends Logging {

  @transient private var _sparkSession: SparkSession = _
  @transient private var _fs: FileSystem = _

  /**
   * Returns a single accessible SparkSession
   * @param props
   * @tparam T this is for serializerClass, which is of type Class[T]
   * @return Spark session
   */
  def getSparkSession[T](props: Properties): SparkSession = {

    val mode = props.getProperty("mode")
    val appName = props.getProperty("appName")

    val conf = new SparkConf()
    conf.setMaster(mode).setAppName(appName)
//    conf.set("fs.azure.account.auth.type."+props.getProperty("dataCoreAdlsAccount")+".dfs.core.windows.net", "OAuth")
//    conf.set("fs.azure.account.oauth.provider.type."+props.getProperty("dataCoreAdlsAccount")+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
//    conf.set("fs.azure.account.oauth2.client.id."+props.getProperty("dataCoreAdlsAccount")+".dfs.core.windows.net", props.getProperty("appId"))
//    conf.set("fs.azure.account.oauth2.client.secret."+props.getProperty("dataCoreAdlsAccount")+".dfs.core.windows.net", props.getProperty("appSecret"))
//    conf.set("fs.azure.account.oauth2.client.endpoint."+props.getProperty("dataCoreAdlsAccount")+".dfs.core.windows.net", s"https://login.microsoftonline.com/${props.getProperty("directoryId")}/oauth2/token")

    _sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    logInfo(s"Created new Spark session")

    sparkSession
  }

  /**
   * Returns a single accessible SparkSession
   * @param appProperty
   * @param keyVaultSecret
   * @return Spark session
   */
  def buildSparkSession(appProperty: AppProperty): SparkSession = {

    val mode = appProperty.mode
    val appName = appProperty.appName



    val conf = new SparkConf()

    conf.setMaster(mode)
      .setAppName(appName)


    _sparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", "5000")
      .getOrCreate()

    logInfo(s"New Spark session successfully created...")

    sparkSession
  }

  def sparkSession: SparkSession = _sparkSession
}
