package com.tools.hadoop.mr.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 注意： 导包时，导入 org.apache.hadoop.mapreduce包下的类（2.0的新api）
 *
 * 1. 自定义的类必须符合 MR 的 Mapper 的规范
 *
 * 2. 在 MR 中，只能处理 key-value 格式的数据
 *      在 KEYIN，VALUEIN： mapper 输入的k-v类型。由当前Job的 InputFormat 的 RecordReader 决定！
 *          封装输入的 key-value 由 RR 自动进行。
 *      KEYOUT，VALUEOUT： mapper 输出的k-v类型： 自定义
 *
 * 3. InputFormat 的作用：
 *     （1）验证输入目录中文件格式，是否符合当前 Job 的要求
 *     （2）生成切片，每个切片都会交给一个 MapTask 处理
 *     （3）提供 RecordReader，由 RR 从切片中读取记录，交给 Mapper 进行处理。
 *   方法：List<InputSplit> getSplits： 切片
 *        RecordReader<K, V> createRecordReader： 创建 RR
 *   默认 hadoop 使用的是 TextInputFormat，TextInputFormat 使用 LineRecordReader！
 *
 * 4. 在 hadoop 中，如果有 Reduce 阶段。
 *              MapTask 处理后的 key-value，只是一个阶段性的结果！
 *              这些 key-value 需要传输到 ReduceTask 所在的机器！
 *              将一个对象通过序列化技术，从文件中读取数据，还原为对象是最快捷的方式！
 *      java的序列化协议： Serializable
 *          特点：（1）不仅保存对象的属性值，类型，还会保存大量的包的结构，子父类和接口的继承信息！
 *               （2）比较重
 *      hadoop开发了一款轻量级的序列化协议： Writable协议
 * */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text out_key = new Text();
    private IntWritable out_value = new IntWritable(1);

    // 针对输入的每个 keyin-valuein 调用一次
    // key为偏移量，value为每一行
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("keyin："+key+"--------keyout："+value);
        String[] words = value.toString().split("\t");
        for (String word : words) {
            out_key.set(word);
            // 写出数据(单词，1)
            context.write(out_key, out_value);
        }
    }
}
