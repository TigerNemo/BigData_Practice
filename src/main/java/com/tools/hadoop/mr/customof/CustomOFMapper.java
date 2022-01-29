package com.tools.hadoop.mr.customof;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 1. 什么时候需要 Reduce
 *  （1）合并
 *  （2）需要对数据排序
 * 2. 没有 Reduce 阶段， key-value 不需要实现序列化
 * */

public class CustomOFMapper extends Mapper<LongWritable, Text, String, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String content = value.toString();

        context.write(content + "\r\n", NullWritable.get());
    }
}
