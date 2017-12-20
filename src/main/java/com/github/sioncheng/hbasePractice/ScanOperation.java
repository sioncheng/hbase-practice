package com.github.sioncheng.hbasePractice;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ScanOperation {

    public static void main(String[] args) throws IOException {
        TestTableHelper.execute(new TestTableHelper.Executor() {
            public void execute(Connection connection, Table table) throws IOException {
                Put put1 = new Put(Bytes.toBytes("rows-1"));
                put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));

                Put put2 = new Put(Bytes.toBytes("rows-2"));
                put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));

                Put put3 = new Put(Bytes.toBytes("rows-3"));
                put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));

                List<Put> puts = new ArrayList<Put>();
                puts.add(put1);
                puts.add(put2);
                puts.add(put3);
                table.put(puts);

                System.out.println("==== scan all");
                Scan scan1 = new Scan();
                ResultScanner scanner1 = table.getScanner(scan1);
                for (Result res : scanner1) {
                    System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))));
                }

                System.out.println("==== scan with starting rows-");
                Scan scan2 = new Scan();
                scan2.setStartRow(Bytes.toBytes("rows-"));
                ResultScanner scanner2 = table.getScanner(scan2);
                for (Result res : scanner2) {
                    System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))));
                }

                System.out.println("==== scan with starting rows-2");
                Scan scan3 = new Scan();
                scan3.setStartRow(Bytes.toBytes("rows-2"));
                ResultScanner scanner3 = table.getScanner(scan3);
                for (Result res : scanner3) {
                    System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))));
                }
            }
        });
    }
}
