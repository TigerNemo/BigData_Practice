package com.tools.hadoop.hdfs;



import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.apache.hadoop.fs.*;

import java.io.IOException;

/**
 * 1. FileSystem：文件系统的抽象基类
 *      有两种实现！（FileSystem的实现取决于fs.defaultFS的配置）
 *      LocalFileSystem：本地文件系统  fs.defaultFS=file:///
 *      DistributedFileSystem：分布式文件系统  fs.defaultFS=hdfs://xxx:9000
 *
 * 2. Configuration：功能是读取配置文件中的参数
 *      Configuration 在读取配置文件的参数时，根据文件名，从类路径按照顺序读取配置文件！
 *          先读取 xxx-default.xml，再读取 xxx-site.xml
 *      Configuration 类一加载，就会默认读取8个配置文件！
 *      将8个配置文件中所有属性，读取到一个 Map 集合中！
 *
 *      也提供了 set(name, value)，来手动设置用户自定义的参数！
 * */


public class HDFSClint {

    // hadoop fs(运行一个通用的用户客户端) -mkdir /xxx
    // 创建一个客户端对象，调用创建目录的方法，路径作为方法的参数传入
    @Test
    public void testMkdir() throws IOException {
        // 创建一个客户端对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        System.out.println(fs.getClass().getName());

        fs.mkdirs(new Path("/idea_mkdir"));
        fs.close();
    }

}
