package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 作用：类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，func的函数类型必须是Iterator[T] => Iterator[U]。假设有N个元素，有M个分区，那么map的函数的将被调用N次,而mapPartitions被调用M次,一个函数一次处理所有分区。
  * 2. 需求：创建一个RDD，使每个元素*2组成新的RDD
  */
object SparkRDD_mapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);

    val rdd = sc.parallelize(Array(1, 2, 3, 4));
    //使每个元素*2组成新的RDD
    val rdd3 = rdd.mapPartitions(e => e.map(_ * 2));
    rdd3.collect().foreach(println);
  }
}
