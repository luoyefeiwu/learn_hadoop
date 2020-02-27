package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 创建第一个RDD
  */
object SparkRDD_union {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 5)
    val rdd2 = sc.parallelize(5 to 10)
    val rdd3 = rdd1.union(rdd2)

    rdd3.collect().foreach(println)

  }
}
