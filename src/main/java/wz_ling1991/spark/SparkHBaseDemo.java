package wz_ling1991.spark;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * docker run -d --net=host --name hbase1.3  harisekhon/hbase:1.3
 * 本例采用的hbase是容器跑的，为了避免端口映射麻烦，这里直接是host模式，如果机器上相关端口被占用，可能会导致启动失败
 * 如果启动中报错请检查配置，
 * 如果配置无误，还报错请在hosts下添加10.231.50.179   143f268bfea6（docker容器id）
 */
@Slf4j
public class SparkHBaseDemo {

    public static void main(String[] args) {

        String tableName = "test_hBase";
        String family = "column1";
        HBaseDemo hBaseDemo = new HBaseDemo(tableName);
        Configuration hConfig = hBaseDemo.getHConfig();

//        hBaseDemo.createTable(tableName,family);
//        hBaseDemo.insertList(tableName, family, 1000000, 10000000);
//        hBaseDemo.scan(tableName);
//        hBaseDemo.delete(tableName);
        spark(hConfig);
    }

    public static void spark(Configuration hConfig) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseDemo").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");
        JavaPairRDD<ImmutableBytesWritable, Result> resultRDD = jsc.newAPIHadoopRDD(hConfig, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        log.warn("count:" + resultRDD.count());
        resultRDD.flatMapToPair(x -> {
            List<Tuple2<String, Integer>> list = new ArrayList();
            for (Cell cell : x._2.rawCells()) {
                String rowName = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                list.add(new Tuple2<>(rowName, 1));
            }
            return list.iterator();
        }).reduceByKey((x1, x2) -> x1 + x2).foreach(x -> log.info(x._1 + ":" + x._2));
        while (true) {
            //为了查看4040界面
        }
    }

}

@Slf4j
class HBaseDemo {

    @Getter
    private Configuration hConfig;

    private Connection connection;

    public HBaseDemo(String tableName) {
        try {
            hConfig = HBaseConfiguration.create();
            hConfig.set("hbase.zookeeper.property.clientPort", "2181");
            hConfig.set("hbase.zookeeper.quorum", "10.231.50.179");
            hConfig.set(TableInputFormat.INPUT_TABLE, tableName);
            connection = ConnectionFactory.createConnection(hConfig);
        } catch (Exception e) {
            log.error("init error", e);
        }
    }

    public void createTable(String tableName, String... familyNames) {
        try {
            Admin hBaseAdmin = connection.getAdmin();
            TableName table = TableName.valueOf(tableName);
            HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
            for (String familyName : familyNames) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            hBaseAdmin.createTable(hTableDescriptor);
            log.info("create table success");
        } catch (Exception e) {
            log.error("create table error:", e);
        }
    }

    public void insertList(String tableName, String familyName, int start, int end) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Put> putList = new ArrayList<>();
            Random r = new Random();
            int count = 0;
            for (int i = start; i < end; i++) {
                Put put = new Put(Bytes.toBytes("rowKey" + i));
                String key = "key-" + r.nextInt(5);
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(key), Bytes.toBytes("value-" + i));
                putList.add(put);
                count++;
                if (count / 1000000 == 1) {
                    table.put(putList);
                    log.info("插入1000000条数据");
                    putList.clear();
                    count = 0;
                }
            }
            table.put(putList);
            log.info("insert data success");
        } catch (Exception e) {
            log.error("insert data failed:", e);
        }
    }

    public void scan(String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner resultScanner = table.getScanner(new Scan());
            for (Result result : resultScanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    this.handleCell(cell);
                }
            }
        } catch (Exception e) {

        }
    }


    public void handleCell(Cell cell) {
        String family = new String(CellUtil.cloneFamily(cell));
        String rowKey = new String(CellUtil.cloneRow(cell));
        String rowName = new String(CellUtil.cloneQualifier(cell));
        String value = new String(CellUtil.cloneValue(cell));
        log.info("column Family={},rowKey={},rowName={},value={}", family, rowKey, rowName, value);
    }


    public void delete(String tableName) {
        try {
            TableName tn = TableName.valueOf(tableName);
            Admin admin = connection.getAdmin();
            admin.disableTable(tn);
            admin.deleteTable(tn);
            log.info("delete table success");
        } catch (Exception e) {
            log.error("delete table failed:", e);
        }
    }

}