package com.tools.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * 自定义上传和下载文件
 * 1. 上传文件时，只上传这个文件的一部分
 * 2. 下载文件时，如何只下载这个文件的某一个块？或只下载文件的某一部分。
 * */

public class CustomUploadAndDownload {
    private FileSystem fs;
    private FileSystem localFs;
    private Configuration conf = new Configuration();

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 分布式文件系统
        fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "tiger");
        // 本地文件系统
        localFs = FileSystem.get(new Configuration());
    }

    @After
    public void close() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }

    // 只上传文件的前10M
    @Test
    public void testCustomUpload() throws IOException {
        // 提供两个 Path，和两个 FileSystem
        Path src = new Path("/home/tiger/hello.txt");
        Path dest = new Path("/hello_10M.txt");

        // 使用本地文件系统中获取的输入流读取本地文件
        FSDataInputStream is = localFs.open(src);

        // 使用 HDFS 的分布式文件系统中获取的输出流，向 dest 路径写入数据
        FSDataOutputStream os = fs.create(dest, true);

        // 1k
        byte[] buffer = new byte[1024];
        // 流中数据的拷贝
        for (int i=0; i<1024*10; i++) {
            is.read(buffer);
            os.write(buffer);
        }

        // 关流
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);
    }

    // 下载第一个块
    @Test
    public void testFirstBLock() throws IOException {
        // 提供两个 Path，和两个 FileSystem
        Path src = new Path("/home/tiger/hello.txt");
        Path dest = new Path("/firstBlock");

        // 使用HDFS的分布式文件系统中获取的输入流，读取HDFS上指定路径的数据
        FSDataInputStream is = fs.open(src);

        // 使用本地文件系统中获取的输出流写入本地文件
        FSDataOutputStream os = localFs.create(dest, true);

        // 1k
        byte[] buffer = new byte[1024];
        // 流中数据的拷贝（128M）
        for (int i=0; i<1024*128; i++) {
            is.read(buffer);
            os.write(buffer);
        }

        // 关流
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);
    }

    // 下载第三个块
    @Test
    public void testSecondBLock() throws IOException {
        // 提供两个 Path，和两个 FileSystem
        Path src = new Path("/home/tiger/hello.txt");
        Path dest = new Path("/firstBlock");

        // 使用HDFS的分布式文件系统中获取的输入流，读取HDFS上指定路径的数据
        FSDataInputStream is = fs.open(src);

        // 使用本地文件系统中获取的输出流写入本地文件
        FSDataOutputStream os = localFs.create(dest, true);

        // 定位到流的指定位置
        is.seek(1024*1024*128*2);

        // 1k
        byte[] buffer = new byte[1024];
        // 流中数据的拷贝（128M）
        for (int i=0; i<1024*128; i++) {
            is.read(buffer);
            os.write(buffer);
        }

        // 关流
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);
    }
}
