package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 作用；使用func先对数据进行处理，按照处理后的数据比较结果排序，默认为正序。
  * 2. 需求：创建一个RDD，按照不同的规则进行排序
  */
object SparkRDD_sortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(2, 1, 3, 4))
    rdd.sortBy(x => x).collect().foreach(print)
    println()
    rdd.sortBy(x => x % 3).collect().foreach(print)
  }
}
