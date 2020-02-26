package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 作用：分组，按照传入函数的返回值进行分组。将相同的key对应的值放入一个迭代器。
  * 2. 需求：创建一个RDD，按照元素模以2的值进行分组
  */
object SparkRDD_groupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);
    val rdd = sc.parallelize(1 to 4)
    val group = rdd.groupBy(_ % 2)
    group.collect().foreach(print)
  }
}
