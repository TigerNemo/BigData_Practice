package com.tools.hbase.hbaseapi;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 1. ���ݵ���ɾ�Ĳ�
 *
 * 2. Put�� ����Ե������ݵ� put ����
 *
 * 3. �� hbase �У����������ݶ����� byte[] ��ʽ���ڣ���Ҫ�ѳ��õ���������תΪ byte[]
 *      hbase �ṩ�� Bytes ������
 *              Bytes.toBytes(x)�� ������������ת byte[]
 *              Bytes.toXxx(x)�� �� byte[] תΪ Xxx ���͡�
 *
 * 4. Get�� ����Ե������ݵ� Get ������
 *      ���õ��в�ѯ����ϸ��Ϣ��
 *          ���ò��ĸ��У� get.addColumn(family, qualifier)
 *          ���ò��ĸ����壺 get.addFamily(family)
 *          ֻ��ĳ��ʱ��������ݣ� get.setTimeStamp(timestamp)
 *          ���÷��ص� versions�� get.setMaxVersions(maxVersions)
 *
 * 5. Result�� scan �� get �ĵ��е����еļ�¼��
 *
 * 6. Cell�� ����һ����Ԫ��hbase �ṩ�� CellUtil.clonexxx(cell)������ȡ cell �е����壬������ֵ���ԡ�
 *
 * 7. ResultScanner�� ���� Result ����ļ��ϡ�
 *
 * 8. delete.addColumn()�� ɾ��ĳ��������У�Ϊ���е����µ� cell�����һ�� type=DELETE �ı�ǣ�ֻ��ɾ�����µ�һ����¼��
 *                        �������ʷ�汾�ļ�¼���޷�ɾ����
 *    delete.addColumns()�� ɾ��ָ���е����а汾�����ݣ�Ϊ��ǰ����һ�� type=DeleteColumn �ı�ǵļ�¼��
 * */

public class DataUtil {

    // �Ȼ�ȡ����� table ����
    public static Table getTable(Connection conn, String tableName, String nsname) throws IOException {
        // ��֤�����Ƿ�Ϸ�
        TableName tn = TableUtil.checkTableName(tableName, nsname);
        if (tn == null) {
            return null;
        }
        // ���� TableName ��ȡ��Ӧ�� Table
        return conn.getTable(tn);
    }

    // put ������rowkey�� ���������������������� value
    public static void put(Connection conn, String tableName, String nsname,
                           String rowkey, String cf, String cq, String value) throws IOException {
        // ��ȡ�����
        Table table = getTable(conn, tableName, nsname);
        if (table == null) {
            return;
        }
        // ����һ�� Put ����
        Put put = new Put(Bytes.toBytes(rowkey));
        // �� put ������ cell ��ϸ����Ϣ
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    // get ���� rowkey
    public static void get(Connection conn, String tableName, String nsname, String rowkey) throws IOException {
        // ��ȡ�����
        Table table = getTable(conn, tableName, nsname);
        if (table == null) {
            return;
        }
        Get get = new Get(Bytes.toBytes(rowkey));

        // ���õ��в�ѯ����ϸ��Ϣ
        Result result = table.get(get);
        parseResult(result);
        table.close();
    }

    // ���� result
    public static void parseResult(Result result) {
        if (result != null) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("���壺 " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        "   ������ " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "   ֵ�� " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    // scan '����'�� {STARTROW => x, STOPROW => x, LIMIT => 1}
    public static void scan(Connection conn, String tableName, String nsname) throws IOException {
        // ��ȡ�����
        Table table = getTable(conn, tableName, nsname);
        if (table == null) {
            return;
        }
        // ���� scan ����
        Scan scan = new Scan();

//        scan.setStartRow(startRow); // ����ɨ�����ʼ��
//        scan.setStopRow(stopRow); // ����ɨ��Ľ�����

        // �����ɨ����
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            parseResult(result);
        }
        table.close();
    }

    // delete '����'����rowkey����[����][��][ts]
    public static void delete(Connection conn, String tableName, String nsname, String rowkey) throws IOException {
        // ��ȡ�����
        Table table = getTable(conn, tableName, nsname);
        if (table == null) {
            return;
        }
        // ���� delete ����
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        // ���� delete ʱ�Ĳ���
        // ɾ��ĳ���������
//        delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age")); // ɾ������һ���汾
        delete.addColumns(Bytes.toBytes("cf1"), Bytes.toBytes("age")); // ɾ�����а汾
        table.delete(delete);
        table.close();
    }
}
