package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1. 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    //2. 创建SparkContext,该对象是提交Spark App的入口
    val sc = new SparkContext(conf);
    //3. 使用 sc 创建 RDD 并相应的 transformation 的 action
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1).sortBy(_._2, false).saveAsTextFile(args(1))
    //4. 关闭连接
    sc.stop()
  }
}
