package com.tools.hadoop.mr.customof;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutPutFormat extends FileOutputFormat<String, NullWritable> {
    @Override
    public RecordWriter<String, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
        return new MyRecordWriter(taskAttemptContext);
    }
}
