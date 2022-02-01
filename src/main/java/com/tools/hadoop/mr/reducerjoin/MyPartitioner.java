package com.tools.hadoop.mr.reducerjoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器：
 * 1. 保证 pid 相同的 key-value 分到同一个区
 * */

public class MyPartitioner extends Partitioner<NullWritable, JoinBean> {
    @Override
    public int getPartition(NullWritable key, JoinBean value, int numPartitions) {
        return (value.getPid().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
