package com.jerry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.Test;

public class HBASEClient {
    Configuration conf = null;

    @Before
    public void before() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.183");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    /**
     * 判断表是否存在
     */
    @Test
    public void tableExists() throws Exception {
        //在HBase中管理、访问表需要先创建HBaseAdmin对象
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf("student"));
        System.out.println(b);
    }

}
