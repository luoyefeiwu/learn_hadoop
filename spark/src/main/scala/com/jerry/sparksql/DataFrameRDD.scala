package com.jerry.sparksql

import java.sql.Struct

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameRDD {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    method1()
  }


  def method1(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WC").setMaster("local[*]");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //将RDD 转成DataFrame
    /*people.txt
    Michael, 29
    Andy, 30
    Justin, 19
    * */
    val people = sc.textFile("D:/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    people.show()
    people.registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >=13AND age <= 19")
    teenagers.show()
    teenagers.map(t => "Name:" + t(0)).collect().foreach(println)
    teenagers.map(t => "Name:" + t.getAs("name")).collect().foreach(println)
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
    sc.stop()
  }

  def method2(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val people = sc.textFile("D:/people.txt")
    // 以字符串的方式定义DataFrame的Schema信息
    val schemaString = "name age"
    //导入需要的类
    import org.apache.spark.sql.types.{StructType, StructField, StringType}

    val schema = StructType(schemaString.split("").map(fieldName => StructField(fieldName, StringType, true)))
    //将RDD转换成Row　　　
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    //将Schema作用到RDD上　　
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //将Data Frame注册成临时表
    peopleDataFrame.registerTempTable("people")
    val results = sqlContext.sql("SELECT name FROM people")
    results.map(t => "Name:" + t(0)).collect().foreach(println)
    sc.stop()
  }
}


