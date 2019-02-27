package bridge.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

/*
 * 多线程管理类
 * 负责管理hbase的读取和mysql的写入
 * @author wuran
 * @Date 2019年2月26日
 *
 */
public class Manager implements Runnable {
    public SparkSession spark = null;
    public Properties properties = null;
    public String url = null;
    public String hbaseTableName = null;
    public String mysqlTableName = null;
    public Configuration conf = null;

    boolean isStop = false;


    Long avgTimestamp = 0L;
    int sleepTime = 5;

    int index = 50;

    int limitNum = 100;

    public Manager() {
        init();
    }

    @Override
    public void run() {
        int i = 0;
        while (!isStop()) {
            i++;
            try {

                Timestamp lastDate = getLastDateFromMysql();
                JavaRDD<Tuple2<ImmutableBytesWritable, Result>> rdd = getDataFromHbase(lastDate);
                //version 1.0
//                List<Sensor> avgSensors = reduceHbaseData(rdd);

                //version 2.0
                List<List<List<Sensor>>> sensorsList = reduceHbaseData2(rdd);
                //reduceHbaseData 数据不足的时候会返回null
                if (sensorsList == null || sensorsList.size() == 0) {
                    print("hbase data not enough.");
                    print("waiting...");
                    Thread.sleep(1000 * getSleepTime());
                    i--;
                    continue;
                }
                List<List<Sensor>> avgSensorsList = getAvgSensors(sensorsList);
                for (List<Sensor> avgs : avgSensorsList) {
                    Long avgTimestamp = Long.parseLong(avgs.get(0).getDate());
                    while (System.currentTimeMillis() < avgTimestamp) {
                        Thread.sleep(500);
                        print("waiting...");
                    }
                    writeMysql(avgs);
                }
//                if (avgSensors == null || avgSensors.size() == 0) {
//                    System.out.println("hbase data not enough.");
//                    System.out.println("waiting...");
//                    Thread.sleep(1000 * getSleepTime());
//                    continue;
//                }
//                //当当前时间大于等于最后一条hbase数据时才插入mysql
//                while (System.currentTimeMillis() < avgTimestamp) {
//                    Thread.sleep(500);
//                    System.out.println("waiting...");
//                }
//                writeMysql(avgSensors);
                print("finish.");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (i >= index) {
                break;
            }
        }
        print("program is end");
        spark.stop();
    }

    public void init() {
        //spark config
        spark = SparkSession
                .builder()
                .appName("HbaseTest")
                .getOrCreate();
        //hbase config
        hbaseTableName = "sensor";
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master");

        //mysql config
        url = "jdbc:mysql://192.168.181.1:3306/jpa_test?useSSL=false&useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true&serverTimezone=GMT%2B8";
        mysqlTableName = "sensor_records";
        properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "root");
        properties.put("driver", "com.mysql.cj.jdbc.Driver");
    }

    /*
     * @Description 从mysql中读取最后一条数据的插入时间
     */
    private Timestamp getLastDateFromMysql() {
        //先从mysql中读取最近的数据
        Timestamp lastDate = null;
        Dataset<Row> sensor_records = spark.read().jdbc(url, mysqlTableName, properties);
        Dataset<Row> lastDateRows = sensor_records.orderBy(sensor_records.col("date").desc()).select("date");
        if (lastDateRows.count() != 0) {
            Row lastDateRow = lastDateRows.first();

            lastDate = lastDateRow.getTimestamp(0);
            print("lastDate:" + lastDate.getTime());

        }
        return lastDate;
    }

    /*
     * @Description 从Hbase中读取数据
     */
    public JavaRDD<Tuple2<ImmutableBytesWritable, Result>> getDataFromHbase(Timestamp lastDate) throws IOException {

        //开始写过滤器
        Scan scan = new Scan();

        List<Filter> filters = new ArrayList<>();
        Filter filter1 = new PageFilter(limitNum);
        filters.add(filter1);

        //只读取比最近时间大的
        if (lastDate != null) {
            //+999是因为返回的没有毫秒数，总是x000
            Filter filter = new RowFilter(CompareFilter.CompareOp.GREATER,
                    new BinaryComparator(Bytes.toBytes(Long.toString(lastDate.getTime() + 999))));
            filters.add(filter);

        } else {
            print("mysql no data");
        }
        Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        scan.setFilter(filterList);


        conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD = spark.sparkContext().newAPIHadoopRDD(conf,
                TableInputFormat.class, ImmutableBytesWritable.class,
                Result.class).toJavaRDD();
        return hbaseRDD;
    }

    /*
     * @Description 处理hbase的数据
     */
    private List<Sensor> reduceHbaseData(JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD) {
        ///version 2.0
        //直接使用mapToPair 分组

        //flatMap首先处理每一行数据
        //把每一行数据都解析为sensor，然后返回所有sensor
        Long num = hbaseRDD.count();
        if (num < 10) {
            return null;
        }

        print("num:" + num);
        JavaRDD<Sensor> sensorJavaRDD = hbaseRDD.flatMap(tuple2 -> {
            //rowKey
            //String rowKey1 = Bytes.toString(tuple2._2.getRow());
            //System.out.println("rowkey1:" + rowKey1);
            //rowKey 也是时间戳字符串
            String rowKey = Bytes.toString(tuple2._1.get());

            //value Returns the value of the first column in the Result.
            //String value = Bytes.toString(tuple2._2.value());
            List<Sensor> sensors = new ArrayList<>();
            List<Cell> cells = tuple2._2.listCells();
            for (Cell cell : cells) {
                //get sensor id
                String sensorId = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String id = sensorId.replace("sensor", "");

                //get sensor value
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                Sensor sensor = new Sensor();
                sensor.setId(id);
                sensor.setValue(value);
                sensor.setDate(rowKey);
                sensors.add(sensor);
            }
            return sensors.iterator();
        });

        //然后切分为键值对
        JavaPairRDD<String, Sensor> ones = sensorJavaRDD.mapToPair(sensor -> new Tuple2<>(sensor.getId(), sensor));

        //然后reduce汇总处理
        JavaPairRDD<String, Iterable<Sensor>> finalRDD = ones.groupByKey();

        //最后处理，求平均值
        print("avg");

        List<Tuple2<String, Iterable<Sensor>>> finalList = finalRDD.collect();
        List<Sensor> avgSensor = new ArrayList<>();
        print("data count:" + finalList.size());
        for (Tuple2<String, Iterable<Sensor>> tuple2 : finalList) {
            Sensor temp = new Sensor();
            temp.setId(tuple2._1);
            System.out.println(temp.getId());
            Integer count = 0;
            Long avgDate = 0L;
            for (Sensor s : tuple2._2) {

                count += Integer.parseInt(s.getValue());
                avgDate = Long.parseLong(s.getDate());
            }

            System.out.println(count);
            System.out.println("avgDate:" + avgDate);

            temp.setValue((count / num) + "");
            temp.setDate(avgDate + "");
            avgSensor.add(temp);
            avgTimestamp = avgDate;
        }
        return avgSensor;
    }

    /*
     * 改为分成多个数组返回
     * 每个元素代表一个sensor， List<List<sensor>>是代表了所有数据，每个元素是10个sensor
     * @version 2.0
     */
    private List<List<List<Sensor>>> reduceHbaseData2(JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD) {
        ///version 2.0
        //直接使用mapToPair 分组

        //flatMap首先处理每一行数据
        //把每一行数据都解析为sensor，然后返回所有sensor
        Long num = hbaseRDD.count();
        if (num < 10) {
            return null;
        }
        Long group = num / 10;

        print("num:" + num);
        JavaRDD<Sensor> sensorJavaRDD = hbaseRDD.flatMap(tuple2 -> {

            //rowKey 也是时间戳字符串
            String rowKey = Bytes.toString(tuple2._1.get());

            List<Sensor> sensors = new ArrayList<>();
            List<Cell> cells = tuple2._2.listCells();
            for (Cell cell : cells) {
                //get sensor id
                String sensorId = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String id = sensorId.replace("sensor", "");

                //get sensor value
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                Sensor sensor = new Sensor();
                sensor.setId(id);
                sensor.setValue(value);
                sensor.setDate(rowKey);
                sensors.add(sensor);
            }
            return sensors.iterator();
        });

        //然后切分为键值对
        JavaPairRDD<String, Sensor> ones = sensorJavaRDD.mapToPair(sensor -> new Tuple2<>(sensor.getId(), sensor));

        //然后reduce汇总处理
        JavaPairRDD<String, Iterable<Sensor>> finalRDD = ones.groupByKey();

        //最后处理，求平均值
        System.out.println("avg");

        List<Tuple2<String, Iterable<Sensor>>> finalList = finalRDD.collect();
        List<Sensor> avgSensor = new ArrayList<>();
        //组织形式：
        //每个key是sensor id,List<List<sensor>>是代表了所有数据，每个元素是10个sensor
        Map<String, List<List<Sensor>>> listMap = new HashMap<>();
        //这种比较合适，直接通过index访问，可以foreach
        List<List<List<Sensor>>> sensorsList = new ArrayList<>();
        for (Tuple2<String, Iterable<Sensor>> tuple2 : finalList) {
//            print(tuple2._1);
            sensorsList.add(splitIterator(tuple2._2.iterator(), group.intValue()));

        }
        return sensorsList;
    }

    /*
     * @Description 把数据写到mysql中
     */
    private void writeMysql(List<Sensor> avgSensor) {
        //构建rows数据
        List<Row> rows = new ArrayList<>();
        for (Sensor s : avgSensor) {
            //这里要用java.sql包下的类型，而且要用timestamp类型，因为date只记录日期，time只记录时间
            java.sql.Timestamp timestamp = new java.sql.Timestamp(Long.parseLong(s.getDate()));
//            Date date = new Date();
//            date.setTime();
            try {
                Row row = RowFactory.create(timestamp, Double.parseDouble(s.getValue()), Integer.parseInt(s.getId()));
                rows.add(row);
            } catch (Exception e) {
                print("parse error, id:" + s.getId());
                e.getStackTrace();
            }

        }

        //构建column类型
        List<StructField> structFields = new ArrayList<>();
        //自增列可以忽略不写，默认值的列忽略不写即可
        structFields.add(DataTypes.createStructField("date", DataTypes.TimestampType, true));
        structFields.add(DataTypes.createStructField("value", DataTypes.DoubleType, true));
        structFields.add(DataTypes.createStructField("sensor_id", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> df = spark.createDataFrame(rows, structType);
        df.write().mode(SaveMode.Append).jdbc(url, mysqlTableName, properties);


    }

    private List<List<Sensor>> splitIterator(Iterator<Sensor> iterator, int group) {
        List<List<Sensor>> sensorsList = new ArrayList<>();
        for (int i = 0; i < group; i++) {
            List<Sensor> sensors = new ArrayList<>();
            int index = 0;
            while (iterator.hasNext()) {
                index++;
                sensors.add(iterator.next());
                if (index == 10) {
                    break;
                }
            }
            sensorsList.add(sensors);
        }
        return sensorsList;
    }

    // L1 sensor 第一个list代表有多少个sensor
    // L2 sensors 按10组一个分 第二个list代表有多少组数据，按10一组分
    // L3 sensorData
    //返回值
    //L1 有多少组
    //L2 每个组里面是sensor的avg
    private List<List<Sensor>> getAvgSensors(List<List<List<Sensor>>> sensorsList) {
        //细化成两层
        //L1 代表有多少个sensor
        //L2 每个sensor有多少个平均数
        List<List<Sensor>> avgSensorsList = new ArrayList<>();
        //最大组数;
        int num = 0;
        for (List<List<Sensor>> sensors : sensorsList) {

            //这里把组全部求出平均值
            List<Sensor> avgSensors = new ArrayList<>();

            //求出最大组数
            if (sensors.size() > num) {
                num = sensors.size();
            }

            //这里得到的是10组一个的sensor
            for (List<Sensor> sensorList : sensors) {
                Sensor temp = new Sensor();

                Integer count = 0;
                Long avgDate = 0L;
                for (Sensor s : sensorList) {
                    temp.setId(s.getId());
                    count += Integer.parseInt(s.getValue());
                    avgDate = Long.parseLong(s.getDate());
                }

                temp.setValue((count / 10) + "");
                temp.setDate(avgDate + "");
                avgSensors.add(temp);

            }
            avgSensorsList.add(avgSensors);
        }

        //开始划分为按组分，每组里面有sensor的形式
        //L1 有多少组
        //L2 每个组里面是sensor的avg
        List<List<Sensor>> finalAvgSensorsList = new ArrayList<>();

        for (int i = 0; i < num; i++) {
            List<Sensor> finalAvgSensors = new ArrayList<>();
            for (List<Sensor> list : avgSensorsList) {
                if (list.size() - 1 >= i) {
                    finalAvgSensors.add(list.get(i));
                }
            }
            finalAvgSensorsList.add(finalAvgSensors);
        }
        return finalAvgSensorsList;
    }

    private void print(Object obj) {
        System.out.println("");
        System.out.println("");
        System.out.println("");

        System.out.println(obj);
        System.out.println("");
        System.out.println("");
        System.out.println("");


    }

    public boolean isStop() {
        return isStop;
    }

    public void setStop(boolean stop) {
        isStop = stop;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }
}
