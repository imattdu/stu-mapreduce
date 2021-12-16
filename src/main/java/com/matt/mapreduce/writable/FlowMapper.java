package com.matt.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author matt
 * @create 2021-12-16 0:51
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();

        // 2. 切割
        //11 	15043685818	192.168.100.8	www.baidu.com	3659	3538	200
        String[] word = line.split("\t");

        String phone = word[1];
        String up = word[word.length - 3];
        String down = word[word.length - 2];

        // 4
        Text outK = new Text(phone);
        FlowBean outV = new FlowBean();
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();

        // 5写出
        context.write(outK, outV);





    }
}
