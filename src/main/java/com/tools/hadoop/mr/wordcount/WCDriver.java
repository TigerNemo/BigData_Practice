package com.tools.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 1. 一旦启动这个线程，运行 Job
 *
 * 2. 本地模式主要用于测试程序是否正确！
 * */

public class WCDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Path inputPath = new Path("/mrinput/wordcount");
        Path outputPath = new Path("/mroutput/wordcount");

        // 作为整个 Job 的配置
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        // YARN上运行
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "localhost");

        // 保证输出目录不存在
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // 创建 Job
        Job job = Job.getInstance(conf);
        // 为 Job 创建一个名字
        job.setJobName("wordcount");

        // 设置 Job
        // 设置 Job 运行的 Mapper， Reducer类型，Mapper，Reducer 输出的 key-value 类型
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        // Job需要根据 Mapper 和 Reducer 输出的key-value类型准备序列化器，通过准备序列化器对输出的key-value进行序列化
        // 如果 Mapper 和 Reducer 输出的 key-value 类型一致，直接设置 Job 最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 运行 job
        job.waitForCompletion(true);
    }
}
