package com.tools.hadoop.mr.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ReduceJoin 需要在 Reduce 阶段实现 Join 功能，一旦数据量大，效率低。
 *
 * 可以使用 MapJoin 解决 ReduceJoin 低效的问题：
 *      每个 MapTask 在 map() 中完成 Join。
 *   注意：
 *      只需要将要 Join 的数据 order.txt 作为切片，让 MapTask 读取。
 *      pd.txt 不以切片形式读入，而直接在 MapTask 中使用 HDFS 下载此文件，
 *      下载后，使用输入流手动读取其中的数据。 在 map() 之前通常是将大文件以切片形式读取，小文件手动读取。
 *
 * MapJoin 可以适用于一个文件大(order.txt)，一个文件小(pd.txt)的场景。
 * 如果两个文件都很大，可以在 ReduceJoin 中使用分区，改变每个ReduceTask处理的数据量来优化。
 * */

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path("/mrinput/mapjoin");
        Path outputPath = new Path("/mroutput/mapjoin");

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
        job.setJobName("mapjoin");

        // 设置 Job
        // 设置 Job 运行的 Mapper， Reducer类型，Mapper，Reducer 输出的 key-value 类型
        job.setMapperClass(MapJoinMapper.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置分布式缓存
        String cachePath[] = {
                "hdfs://localhost:9000/cachefile/pd.txt",
                "file:///home/tiger/pd.txt"
        };
        job.addCacheFile(new Path(cachePath[0]).toUri());  // 缓存文件在 hdfs 中
//        job.addCacheFile(new Path(cachePath[1]).toUri());  // 缓存文件在 本地

        // 取消 reduce 阶段
        job.setNumReduceTasks(0);

        // 运行 job
        job.waitForCompletion(true);
    }
}
