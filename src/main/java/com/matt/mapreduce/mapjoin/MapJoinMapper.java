package com.matt.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.security.auth.callback.CallbackHandler;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author matt
 * @create 2021-12-23 0:58
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String, String> map = new HashMap<>();
    private Text outK = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取缓冲文件
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(new Path(cacheFiles[0]));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] fields = line.split("\t");
            map.put(fields[0], fields[1]);
        }
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String pdName = map.get(fields[1]);
        outK.set(line + "\t" +pdName);

        context.write(outK, NullWritable.get());
    }
}
