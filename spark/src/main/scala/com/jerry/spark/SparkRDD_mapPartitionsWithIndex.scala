package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 作用：类似于mapPartitions，但func带有一个整数参数表示分片的索引值，因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Interator[T]) => Iterator[U]；
  * 2. 需求：创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD
  */
object SparkRDD_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);
    val rdd = sc.parallelize(Array(1, 2, 3, 4));
    var indexRdd = rdd.mapPartitionsWithIndex((index, items) => (items.map((index, _))));
    indexRdd.collect().foreach(println);
  }
}
