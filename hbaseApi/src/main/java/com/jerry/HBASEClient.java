package com.jerry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
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

    /**
     * 获取所以行数据
     */
    @Test
    public void getAllRows() throws IOException {
        TableName tableName = TableName.valueOf("student1");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    /**
     * 删除行
     */
    @Test
    public void deleteRow() throws IOException {
        TableName tableName = TableName.valueOf("student1");
        Table table = connection.getTable(tableName);
        Delete delete = new Delete("10001".getBytes());
        table.delete(delete);
        table.close();
        System.out.println("删除数据");
    }

    /**
     * 获取某一行
     */
    @Test
    public void getRow() throws IOException {
        TableName tableName = TableName.valueOf("student1");
        String rowKey = "10001";
        Table table = connection.getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
        table.close();
    }

    /**
     * 获取某一行指定“列族:列”
     */
    @Test
    public void getRowQualifier() throws IOException {
        String rowKey = "10001";
        String columnFamily = "name";
        String qualifier = "info";
        TableName tableName = TableName.valueOf("student1");
        Table table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));

        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }
}
