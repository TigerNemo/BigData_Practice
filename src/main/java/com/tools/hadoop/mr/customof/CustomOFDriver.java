package com.tools.hadoop.mr.customof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * 自定义输出格式：
 * 过滤输入的 log 日志，包含 xupt 的网站输出到 xupt.log， 不包含 xupt 的网站输出到 other.log
 *
 * */

public class CustomOFDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path("/mrinput/outputformat");
        Path outputPath = new Path("/mroutput/outputformat");

        // 作为整个 Job 的配置
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        // 保证输出目录不存在
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // 创建 Job
        Job job = Job.getInstance(conf);
        // 为 Job 创建一个名字
        job.setJobName("customof");

        // 设置 Job
        // 设置 Job 运行的 Mapper， Reducer类型，Mapper，Reducer 输出的 key-value 类型
        job.setMapperClass(CustomOFMapper.class);


        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置输入和输出格式
        job.setOutputFormatClass(MyOutPutFormat.class);

        // 取消 reduce 阶段
        job.setNumReduceTasks(0);

        // 运行 job
        job.waitForCompletion(true);
    }
}
