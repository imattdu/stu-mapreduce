package com.matt.mapreduce.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author matt
 * @create 2021-12-15 1:28
 */
// KEYIN Reducer 阶段输入key的类型 Text
// VALUEIN, Reducer 输入 IntWritable
// KEYOUT, Reducer 输出   Text
// VALUEOUT Reducer 输出 IntWritable
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // 每一种key会调用一次 a (1, 1, 1)


    /**
     * 功能：
     * @author matt
     * @date 2021/12/15
     * @param key 343
     * @param values 3434
     * @param context 343
     * @return void
    */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // k:a v:(1, 1, 1)
        int sum = 0;
        // 累加
        for (IntWritable value : values) {
            sum +=  value.get();
        }
        IntWritable outV = new IntWritable(sum);
        // 写出
        context.write(key, outV);
    }
}
