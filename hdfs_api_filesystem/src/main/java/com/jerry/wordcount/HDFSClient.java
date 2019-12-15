package com.jerry.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    FileSystem fs;

    @Before
    public void before() throws IOException {
        fs = FileSystem.get(URI.create("hdfs://hadoop01:8020"), new Configuration());
        System.out.println("Before!!!!!!!");
    }

    @After
    public void after() throws IOException {
        fs.close();
        System.out.println("After!!!!!!");
    }

    @Test
    public void put() throws Exception {
        //获取 hdfs 封装对象
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop01:8020"), new Configuration());
        fileSystem.copyFromLocalFile(new Path("d://log.txt"), new Path("/"));
        //关闭操作对象
        fileSystem.close();
    }

    @Test
    public void get() throws Exception {
        //获取 hdfs 封装对象
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop01:8020"), new Configuration());
        fileSystem.copyToLocalFile(new Path("/test"), new Path("d://"));
        //关闭操作对象
        fileSystem.close();
    }

    @Test
    public void rename() throws Exception {
        //获取 hdfs 封装对象
        fs.rename(new Path("/test"), new Path("/test2"));
    }

    @Test
    public void append() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/log.txt"), 1024);
        FileInputStream open = new FileInputStream("d://log.txt");
        IOUtils.copyBytes(open, append, 1024, true);
    }

    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("以下是文件信息");
                System.out.println(fileStatus.getPath().getName());
                System.out.println(fileStatus.getLen());
            } else if (fileStatus.isSymlink()) {
                System.out.println("以下是软连接信息");
            } else if (fileStatus.isDirectory()) {
                System.out.println("以下是文件夹信息");
                System.out.println(fileStatus.getPath().getName());
                System.out.println(fileStatus.getPermission());
            }
        }
    }

    @Test
    public void lsFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            BlockLocation[] blockLocations = file.getBlockLocations();

            System.out.println("===================================");
            System.out.println(file.getPath());

            for (BlockLocation blockLocation : blockLocations) {
                System.out.print("块在:");
                for (String host : blockLocation.getHosts()) {
                    System.out.println(host.toString() + " ");
                }
            }
        }
    }

    @Test
    public void delete() throws IOException {
        boolean result = fs.delete(new Path("/log.txt"), true);
        if (result) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }

}
