package com.tools.hadoop.mr.groupcompare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * 10000001        Pdt_01  222.8
 * */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean out_key = new OrderBean();
    private NullWritable out_value = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");

        out_key.setOrderId(words[0]);
        out_key.setpId(words[1]);
        out_key.setAccount(Double.parseDouble(words[2]));

        context.write(out_key, out_value);
    }
}
