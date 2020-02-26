package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 作用：过滤。返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成。
  * 2. 需求：创建一个RDD（由字符串组成），过滤出一个新RDD（包含”xiao”子串）
  */
object SparkRDD_filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);

    var sourceFilter = sc.parallelize(Array("xiaoming", "xiaojiang", "xiaohe", "dazhi"))
    val filter = sourceFilter.filter(_.contains("xiao"))
    filter.collect().foreach(print)
  }
}
