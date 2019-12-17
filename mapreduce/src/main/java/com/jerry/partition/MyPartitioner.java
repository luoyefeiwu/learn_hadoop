package com.jerry.partition;

import com.jerry.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, FlowBean> {

    /**
     * Get the partition number for a given key (hence record) given the total
     * number of partitions i.e. number of reduce-tasks for the job.
     *
     * <p>Typically a hash function on a all or a subset of the key.</p>
     *
     * @param text          the key to be partioned.
     * @param flowBean      the entry value.
     * @param numPartitions the total number of partitions.
     * @return the partition number for the <code>key</code>.
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        int partition = 4;
        //获取手机号前三位
        String preNum = text.toString().substring(0, 3);
        //判断省份
        if ("136".equals(preNum)) {
            partition = 0;
        } else if ("137".equals(preNum)) {
            partition = 1;
        } else if ("138".equals(preNum)) {
            partition = 2;
        } else if ("139".equals(preNum)) {
            partition = 3;
        }
        return partition;
    }
}
