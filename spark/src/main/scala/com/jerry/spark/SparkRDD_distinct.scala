package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 作用：对源RDD进行去重后返回一个新的RDD。默认情况下，只有8个并行任务来操作，但是可以传入一个可选的numTasks参数改变它。
  * 2. 需求：创建一个RDD，使用distinct()对其去重。
  */
object SparkRDD_distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val distinctRdd = sc.parallelize(List(1, 2, 1, 5, 2, 9, 6, 1))

    val unionRDD = distinctRdd.distinct()
    unionRDD.collect().foreach(print)
  }
}
