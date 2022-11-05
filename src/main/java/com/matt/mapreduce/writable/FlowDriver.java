package com.matt.mapreduce.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author matt
 * @version 2021-12-16 1:13
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // D:\matt\workspace\idea\hadoop\input\inputflow\phone_data.txt

        // D:\matt\workspace\idea\hadoop\input\inputflow\phone_data.txt
        FileInputFormat.setInputPaths(job, new Path("D:\\matt\\workspace\\idea\\hadoop\\input\\inputflow\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\var\\mr\\a21"));
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
