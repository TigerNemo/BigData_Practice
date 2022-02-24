package com.tools.hadoop.hdfs;



import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 1. FileSystem：文件系统的抽象基类
 *      有两种实现！（FileSystem的实现取决于fs.defaultFS的配置）
 *      LocalFileSystem：本地文件系统  fs.defaultFS=file:///
 *      DistributedFileSystem：分布式文件系统  fs.defaultFS=hdfs://xxx:9000
 *
 *   声明用户身份：（这样可以把 core-site.xml 中的 fs.defaultFS 注释掉）
 *      FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "tiger");
 *
 * 2. Configuration：功能是读取配置文件中的参数
 *      Configuration 在读取配置文件的参数时，根据文件名，从类路径按照顺序读取配置文件！
 *          先读取 xxx-default.xml，再读取 xxx-site.xml
 *      Configuration 类一加载，就会默认读取8个配置文件！
 *      将8个配置文件中所有属性，读取到一个 Map 集合中！
 *
 *      也提供了 set(name, value)，来手动设置用户自定义的参数！
 *
 * 3. FileStatus 代表一个文件的状态（文件的属性信息）
 *
 * 4. offset 和 length
 *      offset： 是偏移量，指块在文件中的起始位置
 *      length： 是长度，指块大小
 *      eg: xxx.zip  390M
 *                            length     offset
 *      blk1:   0-128M         128M        0
 *      blk2:   128-256M       128M        128M
 *      ...
 *      blk4:   384-390M       6M          384
 *
 * 5. LocatedFileStatus
 *      LocatedFileStatus 是 FileStatus的子类，除了文件的属性，还有块的位置信息！
 *
 * */


public class HDFSClient {

    private FileSystem fs;
    private Configuration conf = new Configuration();

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 创建一个客户端对象，调用创建目录的方法，路径作为方法的参数传入
        fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "tiger");
//        System.out.println(fs.getClass().getName());
    }

    @After
    public void close() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }

    // 创建目录： hadoop fs -mkdir /xxx
    @Test
    public void testMkdir() throws IOException {
        fs.mkdirs(new Path("/idea_hdfs"));
    }

    // 上传文件： hadoop fs -put 本地文件 hdfs
    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(false, true,
                new Path("/home/tiger/bank.txt"), new Path("/idea_hdfs"));
    }

    // 下载文件： hadoop fs -get hdfs 本地路径
    @Test
    public void testDownLoad() throws IOException {
        fs.copyToLocalFile(false, new Path("/idea_hdfs"), new Path("/home/tiger"),true);
    }

    // 删除文件： hadoop fs -rm -r -f 路径
    @Test
    public void testDelete() throws IOException {
        fs.delete(new Path("/idea_hdfs"), true);
    }

    // 重命名： hadoop fs -mv 源文件 目标文件
    @Test
    public void testRename() throws IOException {
        fs.rename(new Path("/idea_hdfs"), new Path("/idea_hdfs_rename"));
    }

    // 判断当前路径是否存在
    @Test
    public void testIfPathExsits() throws IOException {
        System.out.println(fs.exists(new Path("/idea_hdfs")));
    }

    // 判断当前路径是目录还是文件
    @Test
    public void testFileIsDir() throws IOException {
        Path path = new Path("/idea_hdfs");

        // 判断单个文件
//        FileStatus fileStatus = fs.getFileStatus(path);
//        System.out.println("是否是目录： "+fileStatus.isDirectory());
//        System.out.println("是否是文件： "+fileStatus.isFile());

        // 判断一个目录下其子目录所有文件
        FileStatus[] listStatus = fs.listStatus(path);
        for (FileStatus fileStatus : listStatus) {
            // 获取文件名
            Path filePath = fileStatus.getPath();
            System.out.println(filePath.getName()+"是否是目录： "+fileStatus.isDirectory());
            System.out.println(filePath.getName()+"是否是文件： "+fileStatus.isFile());
        }
    }

    // 获取到文件的块信息
    @Test
    public void testGetBlockInformation() throws IOException {
        Path path = new Path("/idea_hdfs/hello.zip");
        RemoteIterator<LocatedFileStatus> status = fs.listLocatedStatus(path);
        while (status.hasNext()) {
            LocatedFileStatus locatedFileStatus = status.next();
            System.out.println("Owner："+locatedFileStatus.getOwner());
            System.out.println("Group："+locatedFileStatus.getGroup());
            // 块的位置信息
            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println(blockLocation);
                System.out.println("----------------------");
            }
        }
    }

}
