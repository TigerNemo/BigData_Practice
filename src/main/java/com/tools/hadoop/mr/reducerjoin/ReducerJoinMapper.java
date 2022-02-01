package com.tools.hadoop.mr.reducerjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Map 阶段无法完成 Join，只能封装数据，在 Reducer 阶段完成 Join
 * 1. order.txt： 1001    01  1
 *    pd.txt： 01    小米
 *
 * 2. 要求 Mapper 既要能处理 order.txt 里面的数据，也要能处理 pd.txt 里面的数据。
 *      Bean 必须能封装所有的数据
 *
 * 3. Reducer 只需要输出来自于 order.txt 的数据，需要在 Mapper 中对数据打标记，标记数据的来源。
 * 4. 在 Mapper 中需要获取当前切片的来源，根据来源执行不同的封装逻辑
 * */

public class ReducerJoinMapper extends Mapper<LongWritable, Text, NullWritable, JoinBean> {

    private NullWritable out_key = NullWritable.get();
    private JoinBean out_value = new JoinBean();
    private String source;

    // setUp() 在 map() 之前先运行，只运行一次
    @Override
    protected void setup(Context context) {
        InputSplit inputSplit = context.getInputSplit();  // 获取当前切片的数据
        FileSplit split = (FileSplit) inputSplit;
        source = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        // 打标记
        out_value.setSource(source);

        if (source.equals("order.txt")) {
            out_value.setOrderId(words[0]);
            out_value.setPid(words[1]);
            out_value.setAmount(words[2]);
            // 保证所有的属性不为 null
            out_value.setPname("nodata");
        } else {
            out_value.setPid(words[0]);
            out_value.setPname(words[1]);

            out_value.setOrderId("nodata");
            out_value.setAmount("nodata");
        }
        context.write(out_key, out_value);
    }

}
