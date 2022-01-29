package com.tools.hadoop.mr.asc;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 13470253144  180 180 360
 * LongWritable 继承的是 WritableComparable， 排序默认为升序排序
 * */

public class FlowBeanMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private LongWritable out_key = new LongWritable();
    private Text out_value = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");

        // 封装总流量为 key
        out_key.set(Long.parseLong(words[3]));

        out_value.set(words[0]+"\t"+words[1]+"\t"+words[2]);

        context.write(out_key, out_value);
    }
}
