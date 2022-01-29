package com.tools.hadoop.mr.groupcompare;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    /**
     * shuffle 之后的数据：
     * 10000001        Pdt_01  222.8
     * 10000001        Pdt_02  222.8
     * 10000001        Pdt_05  25.8
     *
     * 10000002        Pdt_06  722.4
     * 10000002        Pdt_04  122.4
     * 10000002        Pdt_03  522.8
     *
     * 10000003        Pdt_01  232.8
     * 10000003        Pdt_01  33.8
     *
     * 一组数据会进入一个 Reducer
     *
     * OrderBean key-NullWritable nullWritable 在 reducer 工作期间，
     * 只会实例化一个 key-value 的对象！
     * 每次调用迭代器迭代下个记录时，使用反序列化器从文件中或内存中读取下一个 key-value 数据的值，
     * 封装到之前 OrderBean key-NullWritable nullWritable 在 reducer 的属性中。
     * 即迭代时对象一直都是同一个对象，属性发生了变化
     * */

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Double maxAccount = key.getAccount();
        for (NullWritable nullWritable : values) {
            if (!key.getAccount().equals(maxAccount)) {
                break;
            }
            // 最大值相等也符合条件
            context.write(key, nullWritable);
        }
    }
}
