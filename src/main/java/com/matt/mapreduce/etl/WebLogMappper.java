package com.matt.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author matt
 * @create 2021-12-23 23:39
 */
public class WebLogMappper extends Mapper<LongWritable, Text, Text, NullWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] s = line.split(" ");
        boolean result = parseLog(line, context);
        if (!result) {
            return;
        }
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] fields = line.split(" ");

        if (fields.length > 11) {
            return true;
        }
        return false;

    }
}
