package com.jerry.sparksql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Hive {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("DataSourceApp")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    //通过SparkSQL访问Hive
    hiveTable(hiveContext).show()
    sc.stop()
  }

  def hiveTable(hiveContext: HiveContext) = {
    hiveContext.table("emp")
  }
}
