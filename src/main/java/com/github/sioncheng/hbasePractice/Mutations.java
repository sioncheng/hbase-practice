package com.github.sioncheng.hbasePractice;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Mutations {

    public static void main(String[] args) throws IOException {
        TestTableHelper.execute(new TestTableHelper.Executor() {
            public void execute(Connection connection, Table table) throws IOException {
                Put put = new Put(Bytes.toBytes("row1"));
                put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                        4, Bytes.toBytes("val99"));
                put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual4"),
                        4, Bytes.toBytes("val100"));

                Delete delete = new Delete(Bytes.toBytes("row1"));
                delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));

                RowMutations mutations = new RowMutations(Bytes.toBytes("row1"));
                mutations.add(put);
                mutations.add(delete);
                table.mutateRow(mutations);

                Delete delete1 = new Delete(Bytes.toBytes("row1"));
                Object[] results = new Object[1];
                List<Row> batch = new ArrayList<Row>();
                batch.add(delete1);
                try {
                    table.batch(batch, results);
                } catch (java.lang.InterruptedException ie) {
                    ie.printStackTrace();
                }
                System.out.println(results[0]);
            }
        });
    }
}
