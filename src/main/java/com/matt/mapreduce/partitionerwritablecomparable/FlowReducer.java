package com.matt.mapreduce.partitionerwritablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author matt
 * @create 2021-12-16 1:05
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    FlowBean outV = new FlowBean();

    // flowbean phone
    // phone flowbean
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value, key);
        }



    }
}
