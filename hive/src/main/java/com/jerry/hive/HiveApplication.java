package com.jerry.hive;


import com.jerry.hive.function.HelloUDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HiveApplication {

    public static void main(String[] args) {
        HelloUDF udf = new HelloUDF();
        System.out.println(udf.evaluate(new Text("zhangsan")));
        System.out.println(udf.evaluate(new Text("zhangsan"), new IntWritable(20)));
    }

}
