package com.github.sioncheng.hbasePractice;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class AppendOperation {

    public static void main(String[] args) throws IOException {
        TestTableHelper.execute(new TestTableHelper.Executor() {
            public void execute(Connection connection, Table table) throws IOException {
                Put put = new Put(Bytes.toBytes("row1"));
                put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));

                table.put(put);

                Get get = new Get(Bytes.toBytes("row1"));
                Result result = table.get(get);

                byte[] valBytes = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));

                System.out.println(Bytes.toString(valBytes));

                Append append = new Append(Bytes.toBytes("row1"));
                append.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));

                table.append(append);

                Result result2 = table.get(get);
                byte[] valBytes2 = result2.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));

                System.out.println(Bytes.toString(valBytes2));
            }
        });
    }
}
