package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 作用：以指定的随机种子随机抽样出数量为fraction的数据，withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。
  * 2. 需求：创建一个RDD（1-10），从中选择放回和不放回抽样
  */
object SparkRDD_sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 10)
    var sample1 = rdd.sample(true, 0.4, 2)
    sample1.collect().foreach(print)
    var sample2 = rdd.sample(false, 0.2, 3)
    sample2.collect().foreach(print)
  }
}
