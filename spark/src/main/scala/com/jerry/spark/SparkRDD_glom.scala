package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 作用：将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
  * 2. 需求：创建一个4个分区的RDD，并将每个分区的数据放到一个数组
  */
object SparkRDD_glom {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);

    sc.parallelize(1 to 16, 4)

    val rdd = sc.parallelize(1 to 16, 4)
    rdd.glom().collect().foreach(array => {
      println(array.mkString(","))
    })
  }


}
