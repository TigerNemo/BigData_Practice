package com.tools.hadoop.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 1. 输入：
 *      tiger,a.txt\t3
 *      tiger,b.txt\t3
 * 2. 输出：
 *      tiger,a.txt\t3 b.txt\t3
 * */

public class IndexReducer2 extends Reducer<Text, Text, Text, Text> {

    private Text out_value = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        // 拼接 value
        for (Text value : values) {
            stringBuffer.append(value.toString() + " ");
        }
        out_value.set(stringBuffer.toString());
        context.write(key, out_value);
    }
}
