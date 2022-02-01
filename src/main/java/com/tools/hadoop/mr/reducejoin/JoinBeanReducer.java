package com.tools.hadoop.mr.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * order.txt： 1001    01  1
 * pd.txt： 01    小米
 *       orderid, pid, amount, source, pname
 * (null, 1001, 01, 1, order.txt, nodata)
 * (null, nodata, 01, nodata, pd.txt, 小米)
 *
 * 在输出之前，需要把数据按照 source 属性分类
 * 只能在 reduce 中分类
 * */

public class JoinBeanReducer extends Reducer<NullWritable, JoinBean, NullWritable, JoinBean> {
    // 分类的集合
    private List<JoinBean> orderDatas = new ArrayList<>();
    private Map<String, String> pdDatas = new HashMap<>();

    // 根据 source 分类
    @Override
    protected void reduce(NullWritable key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
        for (JoinBean value : values) {
            if (value.getSource().equals("order.txt")) {
                // 将value对象的属性数据取出，封装到一个新的JoinBean中
                // 不可以直接 orderDatas.add(value)，因为value至始至终都是同一个对象，只不过每次迭代，属性会随之变化
                JoinBean joinBean = new JoinBean();
                try {
                    BeanUtils.copyProperties(joinBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderDatas.add(joinBean);
            } else {
                // 数据来源于 pd.txt
                pdDatas.put(value.getPid(), value.getPname());
            }
        }
    }

    // Join 数据，写出
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // 只输出来自 orderDatas 的数据
        for (JoinBean joinBean : orderDatas) {

            // 从 Map 中根据 pid 取出 pname，设置到 bean 的 pname 属性中
            joinBean.setPname(pdDatas.get(joinBean.getPid()));
            context.write(NullWritable.get(), joinBean);
        }
    }
}
