package com.jerry

import org.apache.spark.{SparkConf, SparkContext}

package object WordCount {
  def main(args:Array[String]):Unit={
    //创建SparkConf 对象，存储应用的配置信息
    val conf=new SparkConf()
    //设置应用程序名称，可以在Spark WebUI中显示
    conf.setAppName("Spark-WordCount")
    //设置集群Master 节点访问地址
    conf.setMaster("spark://hadoop01:7077")

    //
    val sc=new SparkContext(conf)

  }
}
