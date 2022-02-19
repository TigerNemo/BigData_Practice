package com.tools.hadoop.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 1. 输入
 *      tiger-a.txt, 1
 *      tiger-a.txt, 1
 *      tiger-a.txt, 1
 * 2. 输出
 *      tiger-a.txt, 3
 * */

public class IndexReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable out_value = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        out_value.set(sum);
        context.write(key, out_value);
    }
}
