package com.jerry.arrayBuffer2JavaList

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object ArrayBuffer2JavaList {
  def main(args: Array[String]): Unit = {
    // Scala集合和Java集合互相转换
    val arr = ArrayBuffer[Int](1, 2, 3)
    val list: java.util.List[Int] = arr.asJava
    val buffer: scala.collection.mutable.Buffer[Int] = list.asScala
    println(list) //输出 [1, 2, 3]
    println(buffer) //输出 [1, 2, 3]
  }
}
