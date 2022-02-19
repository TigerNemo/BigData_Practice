package com.tools.hadoop.mr.commonfans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * keyin-valuein:  (友\t用户，用户，用户)
 *   map()： 使用 keyin 作为 valueout
 *           将 valuein 切分后，两两拼接，作为 keyout
 * keyout-valueout: (用户-用户，友)
 *                  (A-B,C),(A-B,E)
 *                  (A-E,C),(A-G,C),(A-F,C),(A-K,C)
 *                  (B-E,C),(B-G,C)
 * */

public class FansMapper2 extends Mapper<Text, Text, Text, Text> {

    private Text out_key = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] users = value.toString().split(",");

        // 保证数组中的用户名有序。 防止出现(B-E,C)，(E-B,G)这种key不同但意义相同的情况
        Arrays.sort(users);

        // 将 valuein 切分后，两两拼接，作为 keyout
        for (int i = 0; i < users.length-1; i++) {
            for (int j = i+1; j < users.length; j++) {
                out_key.set(users[i] + "-" + users[j]);
                context.write(out_key, key);
            }
        }
    }
}
