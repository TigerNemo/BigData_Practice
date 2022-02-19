package com.tools.hadoop.mr.commonfans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * keyin-valuein:  (用户：友，友，友)
 *                 (A:B,C,D,F,E,O)
 *  map(): 将 valuein 拆分为若干好友，作为 keyout 写出
 *         将 keyin 作为 valueout
 * keyout-valueout:  (友：用户)
 *                   (C:A),(C:B),(C:E)
 * */

public class FansMapper1 extends Mapper<Text, Text, Text, Text> {

    private Text out_key = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] friends = value.toString().split(",");
        for (String friend : friends) {
            out_key.set(friend);
            context.write(out_key, key);
        }
    }
}
