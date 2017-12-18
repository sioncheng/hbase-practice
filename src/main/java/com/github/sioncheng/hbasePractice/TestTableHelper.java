package com.github.sioncheng.hbasePractice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

class TestTableHelper {

    public interface Executor {
        void execute(Connection connection, Table table) throws IOException;
    }

    public static void execute(Executor executor) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "172.16.25.129");
        //configuration.set("hbase.master", "172.16.25.129:16010");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        cleanRow123(table);

        executor.execute(connection, table);

        table.close();
        connection.close();
    }

    private static void cleanRow123(Table table) throws IOException {
        Delete delete1 = new Delete(Bytes.toBytes("row1"));
        table.delete(delete1);
        Delete delete2 = new Delete(Bytes.toBytes("row2"));
        table.delete(delete2);
        Delete delete3 = new Delete(Bytes.toBytes("row3"));
        table.delete(delete3);
    }
}
