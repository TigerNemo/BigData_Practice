package com.tools.hadoop.mr.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 倒排索引案例（多 job 串联）
 *      有大量的文本（文档，网页），需要建立搜索索引
 *
 *     输入数据                   第一次预期输出结果                 第二次预期输出结果
 *        tiger pingping        tiger--a.txt   3            tiger c.txt-->2 b.txt-->2 a.txt-->3
 * a.txt  tiger ss              tiger--b.txt   2            pingping c.txt-->1 b.txt-->3 a.txt-->1
 *        tiger ss              tiger--c.txt   2            ss c.txt-->1 b.txt-->1 a.txt-->2
 *
 *        tiger pingping        pingping--a.txt  1
 * b.txt  tiger pingping        pingping--b.txt  3
 *        pingping ss           pingping--c.txt  1
 *
 *        tiger ss
 * c.txt  tiger pingping        ss--a.txt  2
 *                              ss--b.txt  1
 *                              ss--c.txt  1
 *
 * 如果一个需求，一个 MRjob 无法完成，可以将需求拆分为若干个 Job，多个 Job 按照依赖关系依次执行。
 * 默认一个 MapTask 只处理一个切片的数据，默认的切片策略，一个切片只属于一个文件
 *
 * 1.需要提交两个 Job
 *      Job2 必须依赖于 Job1， 必须在 Job1 已经运行完成之后，生成结果后，才能运行。
 *
 * 2.JobControl： 定义一组MR Jobs，还可以指定其依赖关系
 *      可以通过 addJob(ControlledJob aJob) 向一个 JobControl 中添加 Job 对象
 *
 * 3.ControlledJob
 *      addDependingJob(ControlledJob dependingJob)： 为当前 Job 添加依赖的 Job
 *      public ControlledJob(Configuration conf)： 基于配置构建一个 ControlledJob
 * */

public class IndexDriver {
    public static void main(String[] args) throws IOException {
        Path inputPath = new Path("/mrinput/index");
        Path outputPath = new Path("/mroutput/index/midindex");
        Path finalOutputPath = new Path("/mroutput/index/finalindex");

        // 作为整个 Job 的配置
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();
        //设置行的分隔符，这里是制表符\t，第一个制表符前面的是Key，第一个制表符后面的内容都是value
        conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "-");

        // 保证输出目录不存在
        FileSystem fs = FileSystem.get(conf1);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        if (fs.exists(finalOutputPath)) {
            fs.delete(finalOutputPath, true);
        }

        // 创建 Job
        Job job1 = Job.getInstance(conf1);
        Job job2 = Job.getInstance(conf2);
        // 为 Job 创建名字
        job1.setJobName("index1");
        job2.setJobName("index2");

        // 设置 Job1
        job1.setMapperClass(IndexMapper1.class);
        job1.setReducerClass(IndexReducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);

        // 设置 Job2
        job2.setMapperClass(IndexMapper2.class);
        job2.setReducerClass(IndexReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        // 设置 Job2 的输入格式
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        // 构建 JobControl
        JobControl jobControl = new JobControl("index");

        // 添加运行的 Job
        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());

        // 指定依赖关系
        controlledJob2.addDependingJob(controlledJob1);

        // 向 jobControl 设置要运行哪些 job
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        // 运行 jobControl
        Thread jobControlThread = new Thread(jobControl);
        // 设置此线程为守护线程
        jobControlThread.setDaemon(true);
        jobControlThread.start();

        // 获取 JobControl 线程的运行状态
        while (true) {
            // 判断整个 jobControl 是否全部运行结束
            if (jobControl.allFinished()) {
                System.out.println(jobControl.getSuccessfulJobList());
                return;
            }
        }
    }
}
