package com.tools.hadoop.mr.flowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowBeanReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean out_value  = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (FlowBean flowBean : values) {
            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();
        }

        out_value.setUpFlow(sumUpFlow);
        out_value.setDownFlow(sumDownFlow);
        out_value.setSumFlow(sumUpFlow + sumDownFlow);

        context.write(key, out_value);
    }
}
