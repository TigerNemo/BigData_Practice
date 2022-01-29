package com.tools.hadoop.mr.groupcompare;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import java.io.IOException;

/**
 * 继承 WritableComparator 或 实现 RawComparator
 *
 * 获取分组比较器，如果没设置默认使用 MapTask 排序时 key 的比较器！
 *      默认的比较器比较策略不符合要求，它会将 orderId 一样且 account 一样的记录才认为是一组的！
 * 自定义分组比较器，只按照 orderId 进行对比，只要 orderId 一样，认为 key 相等，这样可以将 orderId 相同的分到一个组。
 * 在组内取第一个最大的即可！
 * */
public class MyGroupingComparator implements RawComparator<OrderBean> {

    private OrderBean key1 = new OrderBean();
    private OrderBean key2 = new OrderBean();
    private DataInputBuffer buffer = new DataInputBuffer();

    // 负责从缓冲区中解析出要比较的两个key对象，调用 compare(Object o1, Object o2)对两个key进行对比
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            buffer.reset(b1, s1, l1);
            key1.readFields(buffer);

            buffer.reset(b2, s2, l2);
            key2.readFields(buffer);

            buffer.reset(null, 0, 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return compare(key1, key2);
    }

    // Comparable的compare(),实现最终的比较
    @Override
    public int compare(OrderBean o1, OrderBean o2) {
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
