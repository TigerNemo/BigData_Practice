package com.tools.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 1. Reducer 需要符合 hadoop 的 reducer 规范
 *
 * 2. KEYIN， VALUEIN： Mapper 输出的 keyout-valueout
 *      KEYOUT, VALUEOUT： 自定义
 * */

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable out_value = new IntWritable();

    // reduce 一次处理一组数据，key 相同的视为一组
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable intWritable : values) {
            sum += intWritable.get();
        }
        out_value.set(sum);
        context.write(key, out_value);
    }
}
