package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 1. 作用：返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
  * 2. 需求：创建一个1-10数组的RDD，将所有元素*2形成新的RDD
  */
object SparkRDD_map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);
    var source = sc.parallelize(1 to 10);
    //---打印---
    source.collect().foreach(println);
    //将所有元素*2
    val mapadd = source.map(_ * 2);
    mapadd.collect().foreach(println);
  }
}
