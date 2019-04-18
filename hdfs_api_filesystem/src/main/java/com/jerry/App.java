package com.jerry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws Exception {
        //1.获取环境变量
        Configuration configuration = new Configuration();
        //2.创建Path 路径实例
        String dfsPath = "hdfs://192.168.1.181:8020/user/root/";//设置文件目录路径
        Path pathdir = new Path(dfsPath);//设置一个目录路径
        Path file = new Path(pathdir + "/test1.txt");//在目录下创建文件
        //3.创建文件系统实例对象 fs
        FileSystem fs = pathdir.getFileSystem(configuration);
        //4.fs 部分方法演示操作
        fs.mkdirs(pathdir);
        //获取 fs.default.name 参数名,如当前主机 master
        System.out.println("Write to" + configuration.get("fs.default.name"));
        //将本地d盘根目录下 log.txt 文件复制到 HDFS 上 dfspath指定目录下
        fs.copyFromLocalFile(new Path("d://log.txt"), file);
        //将HDFS上dfspath指定目录下的文件复制到了本地
        fs.copyToLocalFile(file, new Path("d://"));

        //文件重命名，成功返回true
        boolean bn = fs.rename(file, new Path("hdfs://192.168.1.181:8020/user/root/renamelog.txt"));
        System.out.println(bn);

        //获取HDFS 集群所有节点名称信息
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        DatanodeInfo[] dataNodeStatus = hdfs.getDataNodeStats();//获取 DataNode状态放入数组中

        for (DatanodeInfo datanodeInfo : dataNodeStatus) {
            //输出 DataNode节点名称
            System.out.println("DataNode节点的名字" + datanodeInfo.getHostName());
        }

        //删除 HDFS 目录及文件
        boolean bndel = fs.delete(pathdir, true);
        //删除成功为true
        System.out.println("删除HDFS 目录及文件:" + bndel);
        //5.释放 fs 持有的Blocks
        fs.close();

    }
}
