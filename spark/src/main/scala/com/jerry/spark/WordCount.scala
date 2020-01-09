package com.jerry.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1. 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    //2. 创建SparkContext,该对象是提交Spark App的入口
    val sc = new SparkContext(conf)
    //3. 使用 sc 创建 RDD 并相应的 transformation 的 action
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1).sortBy(_._2, false).saveAsTextFile(args(1))
    //4. 关闭连接
    sc.stop()
  }


  def text(arr: Array[String]): Unit = {
    //1. 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]");
    //2. 创建SparkContext,该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //读取文件将文件一行一行的读取处理
    val lines: RDD[String] = sc.textFile(arr(0))
    //将一行一行的数据分解成一个个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //为了统计方便将单词进行结构转变
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    //对转换结构后的数据进行分组聚合
    var wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _, 1)
    //将统计结果打印到控制台
    val result: Array[(String, Int)] = wordToSum collect()
    result.foreach(println)
    sc.stop()
  }
}
