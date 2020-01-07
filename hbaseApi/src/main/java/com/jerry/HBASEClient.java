package com.jerry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HBASEClient {
    Connection connection = null;

    @Before
    public void before() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.183");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //在HBase中管理、访问表需要先创建HBaseAdmin对象
        connection = ConnectionFactory.createConnection(conf);
    }

    @After
    public void after() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }


    /**
     * 判断表是否存在
     */
    @Test
    public void tableExists() throws Exception {
        Admin admin = connection.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf("student"));
        System.out.println(b == true ? "存在" : "不存在");
        admin.close();
    }


    /**
     * 创建表
     *
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        String tableName = "student1";
        List<String> columnFamily = Arrays.asList("id", "name", "age");
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("表" + tableName + "已存在");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
    }

    /**
     * 删除表
     */
    @Test
    public void dropTabel() throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("student1");
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("删除成功");
        } else {
            System.out.println("表不存在");
        }
    }

    /**
     * 插入数据
     */
    @Test
    public void addRowData() throws IOException {
        String rowKey = "10001";
        String columnFamily = "name";
        String tableName = "student1";
        String column = "info";
        String value = "张三";
        Table table = connection.getTable(TableName.valueOf(tableName));

        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向Put对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
        System.out.println("插入数据成功");
    }
}
