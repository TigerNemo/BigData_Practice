package com.tools.hadoop.mr.customof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<String, NullWritable> {

    private Path xuptPath = new Path("/mroutput/xupt.log");
    private Path otherPath = new Path("/mroutput/other.log");

    private FSDataOutputStream xuptOS;
    private FSDataOutputStream otherOS;

    private FileSystem fs;

    public MyRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
        Configuration conf = taskAttemptContext.getConfiguration();
        fs = FileSystem.get(conf);
        xuptOS = fs.create(xuptPath);
        otherOS = fs.create(otherPath);
    }

    // 负责将 key-value 写出到文件
    @Override
    public void write(String key, NullWritable nullWritable) throws IOException, InterruptedException {
        if (key.contains("xupt")) {
            xuptOS.write(key.getBytes());
        }else {
            otherOS.write(key.getBytes());
        }
    }

    // 关闭操作
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
        if (xuptOS != null) {
            IOUtils.closeStream(xuptOS);
        }
        if (otherOS != null) {
            IOUtils.closeStream(otherOS);
        }
        if (fs != null) {
            fs.close();
        }
    }
}
