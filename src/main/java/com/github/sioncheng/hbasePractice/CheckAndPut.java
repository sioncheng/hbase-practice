package com.github.sioncheng.hbasePractice;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CheckAndPut {

    public static void main(String[] args) throws IOException {
        TestTableHelper.execute(new TestTableHelper.Executor() {
            public void execute(Connection connection, Table table) throws IOException {
                Put put1 = new Put(Bytes.toBytes("row1"));
                put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));

                boolean result1 = table.checkAndPut(Bytes.toBytes("row1")
                        ,Bytes.toBytes("colfam1")
                        , Bytes.toBytes("qual1")
                        ,null
                        ,put1);

                System.out.println(String.format("Put applied ? %s", result1));

                boolean result2 = table.checkAndPut(Bytes.toBytes("row1")
                        ,Bytes.toBytes("colfam1")
                        , Bytes.toBytes("qual1")
                        ,null
                        ,put1);

                System.out.println(String.format("Put applied ? %s", result2));

                Put put3 = new Put(Bytes.toBytes("row1"));
                put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));

                boolean result3 = table.checkAndPut(Bytes.toBytes("row1")
                        ,Bytes.toBytes("colfam1")
                        , Bytes.toBytes("qual1")
                        ,Bytes.toBytes("val1")
                        ,put3);

                System.out.println(String.format("Put applied ? %s", result3));
            }
        });
    }
}
