package com.jerry.sparksql

import org.apache.avro.ipc.specific.Person
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object DataFrameRDD {

  def main(args: Array[String]): Unit = {
    method1()
  }

  case class Person(name: String, age: Int)

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
}


