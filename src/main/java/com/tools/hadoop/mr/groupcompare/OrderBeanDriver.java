package com.tools.hadoop.mr.groupcompare;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * orderid		pid		account
 * 10000001	    Pdt_01	222.8
 * 10000002	    Pdt_06	722.4
 * 10000001	    Pdt_02	222.8
 * 10000001	    Pdt_05	25.8
 * 10000003	    Pdt_01	232.8
 * 10000003	    Pdt_01	33.8
 * 10000002	    Pdt_04	122.4
 * 10000002	    Pdt_03	522.8
 *
 * 统计同一笔订单中，金额最大的商品记录输出。
 * 在同一笔订单中，对每条记录的金额进行降序排序，最大的排前面
 *
 * （1）orderid 和 account 属性都必须作为 key
 * （2）针对 key，提供 compareTo()，先按照 orderid 排序（升降序都可以），再按照 account（降序）排序
 *
 * */

public class OrderBeanDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path("/mrinput/groupcomparator");
        Path outputPath = new Path("/mroutput/groupcomparator");

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
        job.setJobName("groupcompare");

        // 设置 Job
        // 设置 Job 运行的 Mapper， Reducer类型，Mapper，Reducer 输出的 key-value 类型
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        // Job需要根据 Mapper 和 Reducer 输出的key-value类型准备序列化器，通过准备序列化器对输出的key-value进行序列化
        // 如果 Mapper 和 Reducer 输出的 key-value 类型一致，直接设置 Job 最终的输出类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置自定义的分组比较器
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        // 运行 job
        job.waitForCompletion(true);
    }
}
