package com.tools.hbase.hbaseapi;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 1. 数据的增删改查
 *
 * 2. Put： 代表对单行数据的 put 操作
 *
 * 3. 在 hbase 中，操作的数据都是以 byte[] 形式存在，需要把常用的数据类型转为 byte[]
 *      hbase 提供了 Bytes 工具类
 *              Bytes.toBytes(x)： 基本数据类型转 byte[]
 *              Bytes.toXxx(x)： 从 byte[] 转为 Xxx 类型。
 *
 * 4. Get： 代表对单行数据的 Get 操作！
 *      设置单行查询的详细信息：
 *          设置查哪个列： get.addColumn(family, qualifier)
 *          设置查哪个列族： get.addFamily(family)
 *          只查某个时间戳的数据： get.setTimeStamp(timestamp)
 *          设置返回的 versions： get.setMaxVersions(maxVersions)
 *
 * 5. Result： scan 或 get 的单行的所有的记录。
 *
 * 6. Cell： 代表一个单元格，hbase 提供了 CellUtil.clonexxx(cell)，来获取 cell 中的列族，列名和值属性。
 *
 * 7. ResultScanner： 多行 Result 对象的集合。
 *
 * 8. delete.addColumn()： 删除某个具体的列，为此列的最新的 cell，添加一条 type=DELETE 的标记，只能删除最新的一条记录；
 *                        如果有历史版本的记录，无法删除。
 *    delete.addColumns()： 删除指定列的所有版本的数据，为当前生成一个 type=DeleteColumn 的标记的记录。
 * */

public class DataUtil {

    // 先获取到表的 table 对象
    public static Table getTable(Connection conn, String tableName, String nsname) throws IOException {
        // 验证表名是否合法
        TableName tn = TableUtil.checkTableName(tableName, nsname);
        if (tn == null) {
            return null;
        }
        // 根据 TableName 获取对应的 Table
        return conn.getTable(tn);
    }

    // put 表名，rowkey， 列名（列族名：列名）， value
    public static void put(Connection conn, String tableName, String nsname,
                           String rowkey, String cf, String cq, String value) throws IOException {
        // 获取表对象
        Table table = getTable(conn, tableName, nsname);
        if (table == null) {
            return;
        }
        // 创建一个 Put 对象
        Put put = new Put(Bytes.toBytes(rowkey));
        // 向 put 中设置 cell 的细节信息
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    // get 表名 rowkey
    public static void get(Connection conn, String tableName, String nsname, String rowkey) throws IOException {
        // 获取表对象
        Table table = getTable(conn, tableName, nsname);
        if (table == null) {
            return;
        }
        Get get = new Get(Bytes.toBytes(rowkey));

        // 设置单行查询的详细信息
        Result result = table.get(get);
        parseResult(result);
        table.close();
    }

    // 遍历 result
    public static void parseResult(Result result) {
        if (result != null) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("列族： " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        "   列名： " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "   值： " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    // scan '表名'， {STARTROW => x, STOPROW => x, LIMIT => 1}
    public static void scan(Connection conn, String tableName, String nsname) throws IOException {
        // 获取表对象
        Table table = getTable(conn, tableName, nsname);
        if (table == null) {
            return;
        }
        // 构建 scan 对象
        Scan scan = new Scan();

//        scan.setStartRow(startRow); // 设置扫描的起始行
//        scan.setStopRow(stopRow); // 设置扫描的结束行

        // 结果集扫描器
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            parseResult(result);
        }
        table.close();
    }

    // delete '表名'，‘rowkey’，[列族][列][ts]
    public static void delete(Connection conn, String tableName, String nsname, String rowkey) throws IOException {
        // 获取表对象
        Table table = getTable(conn, tableName, nsname);
        if (table == null) {
            return;
        }
        // 构建 delete 对象
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        // 设置 delete 时的参数
        // 删除某个具体的列
//        delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age")); // 删掉最新一个版本
        delete.addColumns(Bytes.toBytes("cf1"), Bytes.toBytes("age")); // 删掉所有版本
        table.delete(delete);
        table.close();
    }
}
