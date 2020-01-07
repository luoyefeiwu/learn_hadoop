package com.jerry.tool;

import com.jerry.mapper.ScanDataMapper;
import com.jerry.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.util.Tool;

public class HbaseMapperReduceTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //作业
        Job job= Job.getInstance();
        job.setJarByClass(HbaseMapperReduceTool.class);

        // mapper
        TableMapReduceUtil.initTableMapperJob("fruit",new Scan(),ScanDataMapper.class,ImmutableBytesWritable.class,Put.class,job);

        // reduce
        TableMapReduceUtil.initTableReducerJob("fruit_mr",InsertDataReducer.class,job);

        //执行
        boolean b = job.waitForCompletion(true);
        return  b? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
