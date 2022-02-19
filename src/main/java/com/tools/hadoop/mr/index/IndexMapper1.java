package com.tools.hadoop.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 1. 输入：
 *      tiger pingping
 * 2. 输出：
 *      tiger-a.txt, 1
 * */

public class IndexMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private String filename;
    private Text out_key = new Text();
    private IntWritable out_value = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        FileSplit split = (FileSplit) inputSplit;
        filename = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split(" ");
        for (String word : words) {
            out_key.set(word + "-" + filename);
            context.write(out_key, out_value);
        }
    }
}
