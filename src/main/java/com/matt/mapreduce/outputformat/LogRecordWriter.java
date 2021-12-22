package com.matt.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author matt
 * @create 2021-12-21 23:21
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream mattOutput = null;
    private FSDataOutputStream otherOutput = null;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            mattOutput = fs.create(new Path("D:\\var\\mr\\matt.log"));
            otherOutput = fs.create(new Path("D:\\var\\mr\\other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String log = text.toString();
        if (log.contains("guigu")) {
            mattOutput.write(log.getBytes(StandardCharsets.UTF_8));
        } else {
            otherOutput.write(log.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(mattOutput);
        IOUtils.closeStream(otherOutput);
    }
}
