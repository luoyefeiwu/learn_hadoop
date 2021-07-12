package com.jerry.hive.function;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HelloUDF extends UDF {

    public Text evaluate(Text name) {
        return new Text("Hello：" + name);
    }

    public Text evaluate(Text name, IntWritable age) {
        return new Text("Hello：" + name + " , age :" + age);
    }
}
