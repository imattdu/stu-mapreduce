package com.matt.mapreduce.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author matt
 * @create 2021-12-15 1:28
 */
// KEYIN map 阶段输入key的类型 LongWritable
// VALUEIN, map 输入 Text
// KEYOUT, map 输出   Text
// VALUEOUT map 输出 IntWritable
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    // 每一行调用一次map方法
    // map 输入 k 偏移量 v 一行数据
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1获取一行
        String line = value.toString();
        // 2.切割 "aa aa"
        //aa
        //bb
        String[] words = line.split(" ");

        // 3.循环写出
        for (String word : words) {
            outK = new Text(word);
            // 写出
            context.write(outK, outV);

        }
    }
}
