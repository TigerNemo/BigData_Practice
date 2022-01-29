package com.tools.hadoop.mr.desc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 对统计流量的结果， 按照总流量进行降序排序。
 *
 * */

public class FlowBeanDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path("/mroutput/flowbean");
        Path outputPath = new Path("/mroutput/flowbeanDesc");

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
        // 如果不一致，就要分别设置 Job 的输出类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置使用自定义的比较器
        job.setSortComparatorClass(MyDescComparator.class);

        // 运行 job
        job.waitForCompletion(true);
    }
}
