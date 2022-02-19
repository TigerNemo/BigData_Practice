package com.tools.hadoop.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 1. 输入
 *      tiger-a.txt\t3
 *      tiger-b.txt\t3
 *      使用 KeyValueTextInputFormat，可以使用一个分隔符，分隔符之前的作为 key，之后的作为 value
 * 2. 输出
 *      tiger-a.txt\t3
 *      tiger-b.txt\t3
 * */

public class IndexMapper2 extends Mapper<Text, Text, Text, Text> {
}
