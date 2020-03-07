package com.jerry.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ParquetSql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("DataSourceApp")
    val sc = new SparkContext(sparkConf)
    val sqlContext = sqlContext(sc)
    //使用SparkSql访问Parquet文件
    parquetFile(sqlContext)
    sc.stop()
  }

  def parquetFile(sqlContext: SQLContext): Unit = {
    val df = sqlContext.read.parquet("D:/usrs.parquet")
    df.show()
  }
}
