package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  *1. 作用：根据分区数，重新通过网络随机洗牌所有数据。
  * 2. 需求：创建一个4个分区的RDD，对其重新分区
  */
object SparkRDD_repartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 16, 4)
    println("查看分区数:" + rdd.partitions.size)
    //对RDD 重新分区
    val rerdd = rdd.repartition(2)
    //查看新RDD的分区数
    println("查看新分区数:" + rerdd.partitions.size)
  }
}
