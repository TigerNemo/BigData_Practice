package com.tools.hadoop.mr.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * 在 Hadoop 中，hadoop 为 MR 提供了分布式缓存
 *      1.用来缓存一些 Job 运行期间需要的文件（普通文件，jar，归档文件(har)）
 *      2.通过在 Job 的 Configuration 中，使用 URI 代替要缓存的文件
 *      3.分布式缓存会假设当前的文件已经上传到了 HDFS，并且在集群的任意一台机器都可以访问到这个 URI 所代表的文件
 *      4.分布式缓存的高效是由于每个 Job 只会复制一次文件，且可以自动在从节点对归档文件解归档
 * */

public class MapJoinMapper extends Mapper<LongWritable, Text, JoinBean, NullWritable> {

    private JoinBean out_key = new JoinBean();
    private Map<String, String> pdDatas = new HashMap<>();

    // 在 map 之前手动读取 pd.txt 中的内容
    @Override
    protected void setup(Context context) throws IOException{
        // 从分布式缓存中读取数据
        URI[] files = context.getCacheFiles();
        for (URI uri : files) {
            // 缓存文件在本地，可以用new File()来进行读取
//            BufferedReader reader = new BufferedReader(new FileReader(new File(uri)));

            // 从hdfs中读取缓存文件。
            // new File(uri) 可以理解为是 file 协议。 现在是URL("http://....")是不可能拿到文件的，所以要用new InputStreamReader()
            FileSystem fileSystem = FileSystem.get(uri, context.getConfiguration());
            FSDataInputStream inputStream = fileSystem.open(new Path(uri));
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line = "";
            // 循环读取 pd.txt 中的每一行
            while (StringUtils.isNotBlank(line = reader.readLine())) {
                String[] words = line.split("\t");
                pdDatas.put(words[0], words[1]);
            }

            reader.close();
        }
    }

    // 对切片中 order.txt 的数据进行 join，输出
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\t");
        out_key.setOrderId(words[0]);
        out_key.setPname(pdDatas.get(words[1]));
        out_key.setAmount(words[2]);

        context.write(out_key, NullWritable.get());
    }
}
