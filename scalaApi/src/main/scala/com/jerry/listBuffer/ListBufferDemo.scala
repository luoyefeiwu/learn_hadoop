package com.jerry.listBuffer

import scala.collection.mutable.ListBuffer

object ListBufferDemo {
  def main(args: Array[String]): Unit = {
    val lst0 = ListBuffer[Int](1, 2, 3)
    println("lst0(2)=" + lst0(2)) //访问指定元素
    for (item <- lst0) {
      println("item=" + item)
    }

    val lst1 = new ListBuffer[Int]
    lst1 += 4 //lst1(4)
    lst1.append(5) //lst1(4,5)

    println(lst1)

    //++ 表示的是加入的是集合中的各个元素
    lst0 ++= lst1 //lst0(1,2,3,4,5)
    val lst2=lst0++lst1//
    val lst3 = lst0 :+ 5 // 单独加入元素(1,2,3,4,5,5)
    println("lst0=" + lst0)
    println("lst1=" + lst1)
    println("lst2=" + lst2)
    println("lst3=" + lst3)


  }
}
