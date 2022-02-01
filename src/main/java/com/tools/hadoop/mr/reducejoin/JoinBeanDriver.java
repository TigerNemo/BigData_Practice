package com.tools.hadoop.mr.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MR 实现 a xxx join b  on a.xx = b.xx
 *
 * order.txt :  id     pid  amount
 *              1001    01  1
 *              1002    02  2
 *              1003    03  3
 * pd.txt :     pid  pname
 *              01  小米
 *              02  华为
 *              03  格力
 * 将商品信息表中数据根据商品 pid 合并到订单数据表中。
 *
 * 替换的前提是： 相同 pid 的数据，需要分到同一个区
 *      以 pid 为条件分区，pid 相同的分到一个区
 *      0号区： 1001   01  1
 *              01  小米
 *      1号区： 1002   02  2
 *              03  格力
 *
 * 注意： 1.分区时，以 pid 为条件进行分区。
 *      2.两种不同的数据，经过同一个 Mapper 的 map() 处理，因此需要在 map()中
 *          判断切片数据的来源，根据来源执行不同的封装策略
 *      3.一个 Mapper 只能处理一种切片的数据，所以在 Map 阶段无法完成 join 操作，需要在 reduce 中实现 Join
 *      4.在 Map 阶段，封装数据。 自定义的 Bean 需要能够封装，两个切片中的所有的数据。
 *      5.在 Reduce 输出时，只需要将来自与 order.txt 中的数据，将 pid 替换为 pname，而不需要输出所有的 key-value
 *          在 Map 阶段对数据打标记，标记哪些 key-value 属于 order.txt，哪些属于 pd.txt
 * */

public class JoinBeanDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path("/mrinput/reducejoin");
        Path outputPath = new Path("/mroutput/reducejoin");

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
        job.setJobName("reducejoin");

        // 设置 Job
        // 设置 Job 运行的 Mapper， Reducer类型，Mapper，Reducer 输出的 key-value 类型
        job.setMapperClass(ReducerJoinMapper.class);
        job.setReducerClass(JoinBeanReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(JoinBean.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置分区器
        job.setPartitionerClass(MyPartitioner.class);

        // 需要 Join 的数据量过大 order.txt 10亿，pd.txt 100W。 提高MR并行运行的效率
        // Map 阶段： 修改片大小，切的片多， MapTask 运行就多
        // Reduce 阶段： 修改 ReduceTask 数量。
        job.setNumReduceTasks(3);

        // 运行 job
        job.waitForCompletion(true);
    }
}
