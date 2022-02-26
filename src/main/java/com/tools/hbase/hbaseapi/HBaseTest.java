package com.tools.hbase.hbaseapi;

import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseTest {
    private Connection conn;

    @Before
    public void init() throws IOException {
        conn = ConnectionUtil.getConn();
    }

    @After
    public void close() throws IOException {
        ConnectionUtil.close(conn);
    }

    @Test
    public void testListNSs() throws IOException {
        System.out.println(NameSpaceUtil.listNameSpace(conn));
    }

    @Test
    public void testIfExistsNSs() throws IOException {
        System.out.println(NameSpaceUtil.ifNSExists(conn, ""));
    }

    @Test
    public void testCreateNS() throws IOException {
        System.out.println(NameSpaceUtil.createNS(conn, "ns1"));
    }

    @Test
    public void testTablesInNS() throws IOException {
        System.out.println(NameSpaceUtil.getTablesInNameSpace(conn, "default"));
    }

    @Test
    public void testDeleteNS() throws IOException {
        System.out.println(NameSpaceUtil.deleteNS(conn,"ns1"));
    }

    @Test
    public void testTableExists() throws IOException {
        System.out.println(TableUtil.ifTableExists(conn, "t1", null));
    }

    @Test
    public void testCreateTable() throws IOException {
        System.out.println(TableUtil.createTable(conn, "t2", null, "cf1", "cf2"));
    }

    @Test
    public void testDropTable() throws IOException {
        System.out.println(TableUtil.dropTable(conn, "t2", null));
    }

    @Test
    public void testPut() throws IOException {
        DataUtil.put(conn, "t1", null, "b1", "cf1", "name", "jack");
    }

    @Test
    public void testGet() throws IOException {
        DataUtil.get(conn, "t1", null, "r1");
    }

    @Test
    public void testScan() throws IOException {
        DataUtil.scan(conn, "t1" ,null);
    }

    @Test
    public void testDelete() throws IOException {
        DataUtil.delete(conn, "t1", null, "a1");
    }
}
