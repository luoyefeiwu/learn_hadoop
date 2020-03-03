package com.jerry.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object DataFrameSql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc)
    /*people.json
    * {"name":"zhangsan"}
      {"name":"lisi","age":30}
      {"name":"wangwu","age":19}
    * */
    val df = sqlContext.read.json("D:/people.json")
    df.registerTempTable("people")
    sqlContext.sql("select * from people where age>2").show()
  }
}
