package com.github.sioncheng.hbasePractice;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Filters {

    public static void main(String[] args) throws IOException {
        TestTableHelper.execute(new TestTableHelper.Executor() {
            public void execute(Connection connection, Table table) throws IOException {
                cleanAndInsert("rowa", table);
                cleanAndInsert("rowb", table);
                scanWithPrefixFilter("rowa", table);
                scanWithPrefixFilter("rowb", table);
                scanWithPrefixFilter("row", table);
            }
        });
    }




    static void cleanAndInsert(String rowKeyPrefix, Table table) throws IOException {
        final int num = 5;
        List<Delete> deleteList = new ArrayList<Delete>(num);
        for (int i = 0; i < num; i++) {
            Delete delete = new Delete(Bytes.toBytes(String.format("%s-%d", rowKeyPrefix, i)));
            deleteList.add(delete);
        }
        table.delete(deleteList);

        List<Put> puts = new ArrayList<Put>(num);
        for (int i = 0 ; i < num; i++) {
            Put put = new Put(Bytes.toBytes(String.format("%s-%d", rowKeyPrefix, i)));
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes(String.format("val%d", i)));
            puts.add(put);
        }

        table.put(puts);
    }

    static void scanWithPrefixFilter(String rowKeyPrefix, Table table) throws IOException {
        Filter prefixFilter = new PrefixFilter(Bytes.toBytes(rowKeyPrefix));
        Scan scan = new Scan();
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);

        System.out.println(String.format("--------- row key prefix filter %s", rowKeyPrefix));
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
            }
        }
        scanner.close();
    }
}
