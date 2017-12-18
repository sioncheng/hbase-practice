package com.github.sioncheng.hbasePractice;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchPutsWithSomeError {

    public static void main(String[] args) throws IOException {

        TestTableHelper.execute(new TestTableHelper.Executor() {
            public void execute(Connection connection, Table table) throws IOException {
                List<Put> puts = new ArrayList<Put>();

                Put put1 = new Put(Bytes.toBytes("row1"));
                put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val11"));
                puts.add(put1);

                Put put2 = new Put(Bytes.toBytes("row2"));
                put2.addColumn(Bytes.toBytes("colfam11"), Bytes.toBytes("qual1"), Bytes.toBytes("val22"));
                puts.add(put2); //this put will fail

                Put put3 = new Put(Bytes.toBytes("row3"));
                put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val33"));
                puts.add(put3);

                table.put(puts);
            }
        });


    }

}
