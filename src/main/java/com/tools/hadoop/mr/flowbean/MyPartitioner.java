package com.tools.hadoop.mr.flowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器
 *
 * 排序器： 排序器影响的是排序的速度（效率）， QuickSorter
 * 比较器： 比较器影响的是排序的结果
 *
 * KEY, VALUE: Mapper输出的 Key-Value 类型
 * */

public class MyPartitioner extends Partitioner<Text, FlowBean> {
    // 计算分区 numPartitions 为总的分区数， reduceTask 的数量
    // 分区号必须为 int 型的值，且必须符合 0 <= partitionNum < numPartitions
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        // 切割前缀
        String suffix = text.toString().substring(0, 3);

        int partitionNUm = 0;

        switch (suffix) {
            case "136":
                partitionNUm = 1;
                break;
            case "137":
                partitionNUm = 2;
                break;
            case "138":
                partitionNUm = 3;
                break;
            case "139":
                partitionNUm = 4;
                break;

            default:
                break;
        }

        return partitionNUm;
    }
}
