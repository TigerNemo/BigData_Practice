package com.tools.hbase.hbaseapi;


import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 1. 创建和关闭 Connection 对象
 *
 * 2. 如何在 HBase 中创建一个 Configuration 对象
 *      可以使用 HBaseConfiguration.create()， 返回的 Configuration，
 *      既包含 hadoop 8个配置文件的参数，又包含 hbase-default.xml 和 hbase-site.xml 中所有的参数配置
 * */

public class ConnectionUtil {
    // 创建一个 Connection 对象
    public static Connection getConn() throws IOException {
        return ConnectionFactory.createConnection();
    }

    public static void close(Connection conn) throws IOException {
        if (conn != null) {
            conn.close();
        }
    }
}
