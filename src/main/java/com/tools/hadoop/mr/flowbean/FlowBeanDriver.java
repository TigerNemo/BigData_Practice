package com.tools.hadoop.mr.flowbean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 1. 统计手机号(String)的上行流量(long, int)， 下行流量(long, int)， 总流量(long, int)
 *
 * id   手机号 从哪个站点发来的    访问哪个域名  上行流量    下行流量    状态码
 * 1    13736230513 192.196.100.1   www.atguigu.com 2481    24681   200
 *
 *
 * 手机号为key, Bean{上行(long, int)， 下行(long, int)， 总流量(long, int)}为 value
 *
 * */

public class FlowBeanDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path("/mrinput/flowbean");
        Path outputPath = new Path("/mroutput/flowbean");

        // 作为整个 Job 的配置
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

//        // YARN上运行
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.hostname", "localhost");

        // 保证输出目录不存在
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // 创建 Job
        Job job = Job.getInstance(conf);
        // 为 Job 创建一个名字
        job.setJobName("flowbean");

        // 设置 Job
        // 设置 Job 运行的 Mapper， Reducer类型，Mapper，Reducer 输出的 key-value 类型
        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);

        // Job需要根据 Mapper 和 Reducer 输出的key-value类型准备序列化器，通过准备序列化器对输出的key-value进行序列化
        // 如果 Mapper 和 Reducer 输出的 key-value 类型一致，直接设置 Job 最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 运行 job
        job.waitForCompletion(true);
    }
}
