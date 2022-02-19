package com.tools.hadoop.mr.commonfans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 计算共同好友
 * 输入：
 *      A:B,C,D,F,E,O
 *      B:A,C,E,K
 *      C:F,A,D,I
 *      ......
 *      O:A,H,I,J
 *
 * 输出：  A-B： C,E
 *        用户-用户： 共同好友...
 *
 *
 * 思路： 从最终结果向原始数据逆推
 * Job1：
 *      Mapper:
 *              keyin-valuein: (用户:友，友，友)
 *                             (A:B,C,D,F,E,O)
 *              map(): 将 valuein 拆分为若干好友，作为 keyout 写出
 *                      将 keyin 作为 valueout
 *              keyout-valueout: (友:用户)
 *                               (C:A),(C:B),(C:E)
 *     Reducer:
 *              keyin-valuein: (友:用户)
 *                             (C:A),(C:B),(C:E)
 *              reduce():
 *              keyout-valueout: (友:用户，用户，用户，用户)
 *
 * Job2：
 *      Mapper:
 *              keyin-valuein: (友:用户，用户，用户，用户)
 *              map(): 使用 keyin 作为 valueout
 *                      将 valuein 切分后，两两拼接，作为 keyout
 *              keyout-valueout: (用户-用户，友)
 *                               (A-B,C),(A-B,E)
 *                               (A-E,C), (A-G,C), (A-F,C), (A-K,C)
 *                               (B-E,C ),(B-G,C)
 *     Reducer:
 *              keyin-valuein: (A-B,C),(A-B,E)
 *              reduce():
 *              keyout-valueout: (A-B:C,E)
 * */

public class FansDriver {
    public static void main(String[] args) throws IOException {
        Path inputPath = new Path("/mrinput/friend");
        Path outputPath = new Path("/mroutput/friend/midresult");
        Path finalOutputPath = new Path("/mroutput/friend/finalfriend");

        // 作为整个 Job 的配置
        Configuration conf1 = new Configuration();
        //设置行的分隔符，这里是制表符\t，第一个制表符前面的是Key，第一个制表符后面的内容都是value
        conf1.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");
        Configuration conf2 = new Configuration();

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
        job1.setJobName("friend1");
        job2.setJobName("friend2");

        // 设置 Job1
        job1.setMapperClass(FansMapper1.class);
        job1.setReducerClass(FansReducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);

        // 设置 Job1 的输入格式
        job1.setInputFormatClass(KeyValueTextInputFormat.class);

        // 设置 Job2
        job2.setMapperClass(FansMapper2.class);
        job2.setReducerClass(FansReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        // 设置 Job2 的输入格式
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        // 构建 JobControl
        JobControl jobControl = new JobControl("friend");

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
