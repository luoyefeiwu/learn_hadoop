package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  *1. 作用：缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
  * 2. 需求：创建一个4个分区的RDD，对其缩减分区
  */
object SparkRDD_coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 16, 4)
    println("RDD分区数" + rdd.partitions.size)
    //对rdd重新分区
    val coalesceRDD = rdd.coalesce(3)
    println("重新分区后的分区数" + coalesceRDD.partitions.size)

  }
}
