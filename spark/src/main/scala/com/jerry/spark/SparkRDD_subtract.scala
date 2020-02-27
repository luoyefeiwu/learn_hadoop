package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 作用：计算差的一种函数，去除两个RDD中相同的元素，不同的RDD将保留下来
  * 2. 需求：创建两个RDD，求第一个RDD与第二个RDD的差集
  */
object SparkRDD_subtract {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(3 to 8)
    val rdd1 = sc.parallelize(1 to 5)
    rdd.subtract(rdd1).collect().foreach(println)

  }
}
