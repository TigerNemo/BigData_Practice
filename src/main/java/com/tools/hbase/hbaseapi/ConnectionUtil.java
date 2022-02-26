package com.tools.hbase.hbaseapi;


import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 1. �����͹ر� Connection ����
 *
 * 2. ����� HBase �д���һ�� Configuration ����
 *      ����ʹ�� HBaseConfiguration.create()�� ���ص� Configuration��
 *      �Ȱ��� hadoop 8�������ļ��Ĳ������ְ��� hbase-default.xml �� hbase-site.xml �����еĲ�������
 * */

public class ConnectionUtil {
    // ����һ�� Connection ����
    public static Connection getConn() throws IOException {
        return ConnectionFactory.createConnection();
    }

    public static void close(Connection conn) throws IOException {
        if (conn != null) {
            conn.close();
        }
    }
}
