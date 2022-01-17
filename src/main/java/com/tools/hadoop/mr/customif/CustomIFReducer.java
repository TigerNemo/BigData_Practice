package com.tools.hadoop.mr.customif;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomIFReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
}
