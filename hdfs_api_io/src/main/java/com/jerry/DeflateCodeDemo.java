package com.jerry;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.CompressionCodec;


/**
 * CompressionCodec 接口实现压缩Demo
 */
public class DeflateCodeDemo {


    public void CodeDemo() throws Exception {

        String inpath = "home/user/comments.xml";
        String outpath = "/home/user/comments.deflate";

        Configured conf = new Configured();
        String codecClassname = "org.apache.hadoop.io.compress.DeflateCodec";
        Class<?> codeClass = Class.forName(codecClassname);

        //创建压缩类型的实例的第一种写法
        //CompressionCodec codec=ReflectUtil.ne
    }

}
