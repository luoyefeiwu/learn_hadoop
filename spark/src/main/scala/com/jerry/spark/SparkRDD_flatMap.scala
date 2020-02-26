package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 作用：类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素）
  * 2. 需求：创建一个元素为1-5的RDD，运用flatMap创建一个新的RDD，新的RDD为原RDD的每个元素的2倍（2，4，6，8，10）
  */
object SparkRDD_flatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);
    val sourceFlat = sc.parallelize(1 to 5)
    val flatMap = sourceFlat.flatMap(1 to _)
    flatMap.collect().foreach(println(_))
  }
}
