package com.tools.hadoop.mr.flowbean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FlowBeanMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text out_key = new Text();
    private FlowBean out_value = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");

        // 封装手机号
        out_key.set(words[1]);
        // 封装上行流量
        out_value.setUpFlow(Long.parseLong(words[words.length-3]));
        // 封装下行流量
        out_value.setDownFlow(Long.parseLong(words[words.length-2]));

        context.write(out_key, out_value);
    }
}
