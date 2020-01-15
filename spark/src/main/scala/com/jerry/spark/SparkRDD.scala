package com.jerry.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD {
  def main(args: Array[String]): Unit = {
    //1. 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2))
    //rdd.collect().foreach(println)
    val mkrdd: RDD[Int] = sc.makeRDD(Array(9, 10))
    //mkrdd.collect().foreach(println)
    //将所有元素*2
    println("将所有元素*2")
    sc.parallelize(1 to 10).map(_ * 2).collect().foreach(println);

    //创建一个RDD，使每个元素*2组成新的RDD
    println("创建一个RDD，使每个元素*2组成新的RDD")
    sc.parallelize(1 to 10).mapPartitions(x => x.map(_ * 2)).collect().foreach(println)

    //创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD
    println("创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD")
    sc.parallelize(1 to 4).mapPartitionsWithIndex((index, item) => (item.map((index, _)))).collect().foreach(println(_))
  }

}
