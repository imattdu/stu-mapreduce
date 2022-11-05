package com.matt.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author matt
 * @create 2021-12-15 1:29
 */
public class WordCountDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        // 1.获取jod
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        // 3.关联 mapper reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4.设置map输出的 k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出的 k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6 设置输入路径 输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\matt\\workspace\\idea\\hadoop\\input\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\var\\mr\\partitoner"));

        job.setNumReduceTasks(2);

        // 7 提交job
        // 监控并打印job信息
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


}
