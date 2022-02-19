package com.tools.hadoop.mr.commonfans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FansReducer2 extends Reducer<Text, Text, Text, Text> {

    private Text out_value = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        for (Text value : values) {
            stringBuffer.append(value.toString() + ",");
        }
        out_value.set(stringBuffer.toString());
        context.write(key, out_value);
    }
}
