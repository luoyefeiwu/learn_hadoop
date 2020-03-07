package com.jerry.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Mysql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("DataSourceApp")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    jdbcTable(sqlContext).show()
    sc.stop()
  }

  def jdbcTable(sqlContext: SQLContext) = {
    sqlContext.read.format("jdbc").options(Map
    ("url" -> "jdbc:mysql://hadoop000:3306/hive?user=root&password=root",
      "dbtable" -> "TBLS",
      "driver" -> "com.mysql.jdbc.Driver")
    ).load()
  }
}
