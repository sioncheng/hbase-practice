package com.github.sioncheng.hbasePractice;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CRUD {

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

                Get get2 = new Get(get);
                get2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));
                Result result2 = table.get(get2);

                System.out.println(result2);

                Get get3 = new Get(get);
                get3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));
                get3.setCheckExistenceOnly(true);
                Result result3 = table.get(get3);

                System.out.println(result3);


                Put put2 = new Put(Bytes.toBytes("row1"));
                put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
                table.put(put2);

                Get get4 = new Get(get);
                Result result4 = table.get(get4);
                System.out.println(result4);

                Delete delete = new Delete(Bytes.toBytes("row1"));
                table.delete(delete);

            }
        });
    }
}
