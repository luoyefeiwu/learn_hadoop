package com.jerry.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrame {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("D:/people.json")
    df.show
    df.printSchema()
    df.select("name").show()
    //使用select方法选择所需要的字段，并为age字段加1　
    df.select(df("name"), df("age") + 1).show()
    //使用filter方法完成条件过滤　
    df.filter(df("age") > 21).show()
    //使用group By方法进行分组，求分组后的总数
    df.groupBy("age").count().show()
    sc.stop
  }
}
