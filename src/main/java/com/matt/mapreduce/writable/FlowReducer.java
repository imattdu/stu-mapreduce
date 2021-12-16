package com.matt.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec;

import java.io.IOException;

/**
 * @author matt
 * @create 2021-12-16 1:05
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        // 1 构造数据 1 《1， 2， 3》

        // total greater
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }


        // 构造kv
        outV.setUpFlow(sumUpFlow);
        outV.setDownFlow(sumDownFlow);
        outV.setSumFlow();

        // 写出
        context.write(key, outV);


    }
}
