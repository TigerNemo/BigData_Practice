package com.tools.hadoop.mr.customif;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * 将多个小文件合并成一个 SequenceFile 文件（SequenceFile 文件是 hadoop 用来存储二进制形式的 key-value 对的文件格式），
 * SequenceFile 里面存储着多个文件，存储的形式为文件路径+名称为 key， 文件内容为 value。
 *
 * 自定义 InputFormat 步骤：
 *  （1） 自定义一个类继承 FileInputFormat。
 *  （2） 改写 RecordReader，实现一次读取一个完整文件封装为 key-value(bytes)形式。
 *        将文件的文件名作为 key。
 *        将文件的内容读取封装为 bytes 类型。
 *        重写 isSplitable()，返回false，让文件不可切，整个文件作为1片。
 *        在RR中，nextKeyValue()是最重要的方法，返回当前读取到的key-value，如果读到返回true，否则返回false。
 *        返回true，调用Mapper的map()来处理。
 *  （3） 在输出时使用 SequenceFileOutPutFormat 输出合并文件。（默认的输出格式是 TextOutputFormat，文本格式）
 *
 *  是否需要 Reduce ：
 *      （1） 是否需要合并
 *              什么时候不需要合并?   仅有一个MapTask，且MapTask不存在相同key的数据！
 *              有多个MapTask，最终期望生成一个结果文件，需要汇总，需要有Reduce。
 *      （2） 是否将结果进行排序
 *              没有Reduce: MapTask----map
 *              有Reduce: MapTask----map-----sort---ReduceTask----copy---sort----reduce
 * */

public class CustomIFDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path("/mrinput/custom");
        Path outputPath = new Path("/mroutput/custom");

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
        job.setJobName("custom");

        // 设置 Job
        // 设置 Job 运行的 Mapper， Reducer类型，Mapper，Reducer 输出的 key-value 类型
        job.setMapperClass(CustomIFMapper.class);
        job.setReducerClass(CustomIFReducer.class);

        // Job需要根据 Mapper 和 Reducer 输出的key-value类型准备序列化器，通过准备序列化器对输出的key-value进行序列化
        // 如果 Mapper 和 Reducer 输出的 key-value 类型一致，直接设置 Job 最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置输入和输出格式
        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 运行 job
        job.waitForCompletion(true);
    }
}
