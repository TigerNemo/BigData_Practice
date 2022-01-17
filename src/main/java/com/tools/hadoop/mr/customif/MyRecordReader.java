package com.tools.hadoop.mr.customif;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 模仿LineRecordReader的实现，重写下列的方法。
 * RecordReader 从 MapTask 处理的当前切片中读取数据
 *
 * XXXContext 都是 Job 的上下文，通过 XXXContext 可以获取 Job 的配置 Configuration 对象
 * */

public class MyRecordReader extends RecordReader {

    private Text key;
    private BytesWritable value;

    private String filename;
    private int length;

    private FileSystem fs;
    private Path path;

    private FSDataInputStream is;

    private boolean flag = true;

    // MyRecordReader 在创建后，在进入 Mapper 的 run() 之前，自动调用
    // 文件的所有内容设置为 1 个切片，切片的长度等于文件的长度
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        filename = fileSplit.getPath().getName();
        length = (int) fileSplit.getLength();
        path = fileSplit.getPath();
        // 获取当前 Job 的配置对象
        Configuration conf = taskAttemptContext.getConfiguration();
        // 获取当前 Job 使用的文件系统
        fs = FileSystem.get(conf);

        is  = fs.open(path);
    }

    // 读取一组输入的 key-value， 读到返回 true，否则返回 false
    // 将文件的名称封装为 key，将文件的内容封装为 BytesWritable 类型的 value，返回 true
    // 第二次调用 nextKeyValue() 返回 false
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (flag) {
            // 实例化对象
            if (key == null) {
                key = new Text();
            }
            if (value == null) {
                value = new BytesWritable();
            }

            // 赋值
            // 将文件名封装到 key 中
            key.set(filename);

            // 将文件的内容读取到 BytesWritable 中
            byte[] content = new byte[length];

            IOUtils.readFully(is, content, 0, length);

            value.set(content, 0, length);

            flag = false;

            return true;
        }
        return false;
    }

    // 返回当前读取到的 key-value 中的 key
    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    // 返回当前读取到的 key-value 中的 value
    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    // 返回读取切片的进度
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    // 在 Mapper 的输入关闭时调用，清理工作
    @Override
    public void close() throws IOException {
        if (is != null) {
            IOUtils.closeStream(is);
        }
        if (fs != null) {
            fs.close();
        }
    }
}
