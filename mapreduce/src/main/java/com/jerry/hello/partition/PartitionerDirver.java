package com.jerry.hello.partition;

import com.jerry.hello.flow.FlowBean;
import com.jerry.hello.flow.FlowMapper;
import com.jerry.hello.flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionerDirver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建job
        Job job = Job.getInstance(new Configuration());
        //2.设置类路径
        job.setJarByClass(PartitionerDirver.class);
        //3. 设置map和reduce
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setNumReduceTasks(5);
        //4. 设置mapper 和 reducer的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5. 设置输入输出数据
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交我们的job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
