package com.jerry.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  *1. 作用：管道，针对每个分区，都执行一个shell脚本，返回输出的RDD。
  * 注意：脚本需要放在Worker节点可以访问到的位置
  * 2. 需求：编写一个脚本，使用管道将脚本作用于RDD上。
  */
object SparkRDD_pipe {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //Shell脚本
    //#!/bin/sh
    //echo "AA"
    //while read LINE; do
    //   echo ">>>"${LINE}
    //done

    val rdd1 = sc.parallelize(List("hi", "Hello", "how", "are", "you"), 1)
    rdd1.pipe("/opt/module/spark/pipe.sh").collect().foreach(print)

    val rdd2 = sc.parallelize(List("hi", "Hello", "how", "are", "you"), 2)
    rdd2.pipe("/opt/module/spark/pipe.sh").collect().foreach(print)
  }
}
