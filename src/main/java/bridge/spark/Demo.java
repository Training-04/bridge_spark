//package bridge.spark;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.filter.*;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.sql.*;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//import scala.Tuple2;
//
//import java.io.IOException;
//import java.sql.Timestamp;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.List;
//import java.util.Properties;
//
//public class Demo {
//    public static SparkSession spark = null;
//    public static Properties properties = null;
//    public static String url = null;
//    public static String hbaseTableName = null;
//    public static String mysqlTableName = null;
//    public static Configuration conf = null;
//    public static void demo() throws IOException {
//        spark = SparkSession
//                .builder()
//                .appName("HbaseTest")
//                .getOrCreate();
////        SparkConf sparkConf = new SparkConf().setAppName("HbaseTest").setMaster("yarn-client").
////                set("spark.testing.reservedMemory","50000000");
//        //set("spark.testing.memory","100000000").
//
////        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
//        readMysql();
//
//
//
//        ///version 2.0
//        //直接使用mapToPair 分组
//
////        //flatMap首先处理每一行数据
////        //把每一行数据都解析为sensor，然后返回所有sensor
////        JavaRDD<Sensor> sensorJavaRDD = hBaseRDD.flatMap(new FlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, Sensor>() {
////            public Iterator<Sensor> call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws Exception {
////                //rowKey
////                String rowKey1 = Bytes.toString(tuple2._2.getRow());
////                System.out.println("rowkey1:" + rowKey1);
////                //rowKey 也是时间戳字符串
////                String rowKey = Bytes.toString(tuple2._1.get());
////                System.out.println("rowkey:" + rowKey);
////
////                //value Returns the value of the first column in the Result.
////                //String value = Bytes.toString(tuple2._2.value());
////                List<Sensor> sensors = new ArrayList<Sensor>();
////                List<Cell> cells = tuple2._2.listCells();
////                for (Cell cell : cells) {
////                    //get sensor id
////                    String sensorId = Bytes.toString(cell.getQualifierArray());
////                    String id = sensorId.replace("sensor", "");
////
////                    //get sensor value
////                    String value = Bytes.toString(cell.getValueArray());
////                    Sensor sensor = new Sensor();
////                    sensor.setId(id);
////                    sensor.setValue(value);
////                    sensor.setDate(rowKey);
////                    sensors.add(sensor);
////                }
////                return sensors.iterator();
////            }
////        });
////
////        //然后切分为键值对
////        JavaPairRDD<String, Sensor> ones = sensorJavaRDD.mapToPair(new PairFunction<Sensor, String, Sensor>() {
////            public Tuple2<String, Sensor> call(Sensor sensor) throws Exception {
////
////                return new Tuple2<String, Sensor>(sensor.getId(), sensor);
////            }
////        });
////
////        //然后reduce汇总处理
////        JavaPairRDD<String, Iterable<Sensor>> finalRDD = ones.groupByKey();
////
////        //最后处理，求平均值
////        System.out.println("avg");
////
////        List<Tuple2<String, Iterable<Sensor>>> finalList = finalRDD.collect();
////        Map<String, Sensor> avgSensor = new HashMap<String, Sensor>();
////        for (Tuple2<String, Iterable<Sensor>> tuple2 : finalList) {
////            Sensor temp = new Sensor();
////            temp.setId(tuple2._1);
////            Integer count = 0;
////            Long avgDate = 0L;
////            for (Sensor s : tuple2._2) {
////                count += Integer.parseInt(s.getValue());
////                avgDate += Long.parseLong(s.getDate());
////            }
////            System.out.println(count);
////            temp.setValue((count/10.0)+"");
////            temp.setDate(avgDate+"");
////            avgSensor.put(temp.getId(),temp);
////        }
////
////        //output
////        System.out.println("begin output");
////        for(Sensor s : avgSensor.values()){
////            System.out.println("id:"+s.getId()+" value:"+s.getValue());
////        }
//
//        ///version 1.0
//        //从result中取出数据
////        JavaRDD<List<Sensor>> rdd = hBaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, List<Sensor>>() {
////            public List<Sensor> call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws Exception {
////                //rowKey
////                String rowKey1 = Bytes.toString(tuple2._2.getRow());
////                System.out.println("rowkey1:"+rowKey1);
////                //rowKey 也是时间戳字符串
////                String rowKey = Bytes.toString(tuple2._1.get());
////                System.out.println("rowkey:"+rowKey);
////
////                //value Returns the value of the first column in the Result.
////                //String value = Bytes.toString(tuple2._2.value());
////                List<Sensor> sensors = new ArrayList<Sensor>();
////                List<Cell> cells = tuple2._2.listCells();
////                for (Cell cell : cells) {
////                    //get sensor id
////                    String sensorId = Bytes.toString(cell.getQualifierArray());
////                    String id = sensorId.replace("sensor", "");
////
////                    //get sensor value
////                    String value = Bytes.toString(cell.getValueArray());
////                    Sensor sensor = new Sensor();
////                    sensor.setId(id);
////                    sensor.setValue(value);
////                    sensor.setDate(rowKey);
////                    sensors.add(sensor);
////                }
////
////                return sensors;
////            }
////        });
////
////        //把sensor分类
////        List<List<Sensor>> sensorList = rdd.collect();
////        Map<String, List<Sensor>> sensorMap = new HashMap<String, List<Sensor>>();
////        for (List<Sensor> sensors : sensorList) {
////            for (Sensor sensor : sensors) {
////                if (sensorMap.containsKey(sensor.getId())) {
////                    sensorMap.get(sensor.getId()).add(sensor);
////                } else {
////                    List<Sensor> list = new ArrayList<Sensor>();
////                    list.add(sensor);
////                    sensorMap.put(sensor.getId(), list);
////                }
////            }
////        }
//
//
//    }
//    public static void init(){
//        //spark config
//        spark = SparkSession
//                .builder()
//                .appName("HbaseTest")
//                .getOrCreate();
//        //hbase config
//        hbaseTableName="sensor";
//        conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","master");
//
//        //mysql config
//        url = "jdbc:mysql://192.168.181.1:3306/jpa_test?useSSL=false&useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true&serverTimezone=GMT%2B8";
//        mysqlTableName = "sensor_records";
//        properties = new Properties();
//        properties.put("user","root");
//        properties.put("password","root");
//        properties.put("driver","com.mysql.cj.jdbc.Driver");
//    }
//    public static void demo()throws IOException{
//        //hbase config
//
//
//
//
//        //先从mysql中读取最近的数据
//        Dataset<Row> sensor_records= spark.read().jdbc(url,mysqlTableName,properties);
//        Row lastDateRow = sensor_records.orderBy(sensor_records.col("date").desc()).select("date").first();
//        Timestamp lastDate = lastDateRow.getTimestamp(0);
//        //开始写过滤器
//        Scan scan = new Scan();
//
//        //只读取比最近时间大的
//        if(lastDate != null){
//            List<Filter> filters = new ArrayList<>();
//            Filter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
//                    new BinaryComparator(Bytes.toBytes(Long.toString(lastDate.getTime()))));
//            Filter filter1 = new PageFilter(10);
//            filters.add(filter);
//            filters.add(filter1);
//
//            Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,filters);
//
//            scan.setFilter(filterList);
////            scan.setLimit(10);
//            System.out.println();
//            System.out.println();
////            System.out.println("limit:"+scan.getLimit());
//            System.out.println();
//            System.out.println();
//
//
////            scan.setCaching(10);
////            scan.setReadType(Scan.ReadType.STREAM);
//
//
////            scan.setTimeRange(lastDate.getTime(),lastDate.getTime()+1000*11);
//        }
//
//
//        conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
//        conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
//        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD = spark.sparkContext().newAPIHadoopRDD(conf,
//                TableInputFormat.class, ImmutableBytesWritable.class,
//                Result.class).toJavaRDD();
//        reduceHbaseData(hbaseRDD);
//    }
//    private static void reduceHbaseData(JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD){
//        ///version 2.0
//        //直接使用mapToPair 分组
//
//        //flatMap首先处理每一行数据
//        //把每一行数据都解析为sensor，然后返回所有sensor
//        Long num = hbaseRDD.count();
//        if(num<10){
//            return ;
//        }
//        System.out.println("num:"+hbaseRDD.count());
//        JavaRDD<Sensor> sensorJavaRDD = hbaseRDD.flatMap(tuple2 -> {
//            //rowKey
//            //String rowKey1 = Bytes.toString(tuple2._2.getRow());
//            //System.out.println("rowkey1:" + rowKey1);
//            //rowKey 也是时间戳字符串
//            String rowKey = Bytes.toString(tuple2._1.get());
//
//            //value Returns the value of the first column in the Result.
//            //String value = Bytes.toString(tuple2._2.value());
//            List<Sensor> sensors = new ArrayList<>();
//            List<Cell> cells = tuple2._2.listCells();
//            for (Cell cell : cells) {
//                //get sensor id
//                String sensorId = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
//                String id = sensorId.replace("sensor", "");
//
//                //get sensor value
//                String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
//
//                Sensor sensor = new Sensor();
//                sensor.setId(id);
//                sensor.setValue(value);
//                sensor.setDate(rowKey);
//                sensors.add(sensor);
//            }
//            return sensors.iterator();
//        });
//
//        //然后切分为键值对
//        JavaPairRDD<String, Sensor> ones = sensorJavaRDD.mapToPair(sensor -> new Tuple2<>(sensor.getId(),sensor));
//
//        //然后reduce汇总处理
//        JavaPairRDD<String, Iterable<Sensor>> finalRDD = ones.groupByKey();
//
//        //最后处理，求平均值
//        System.out.println("avg");
//
//        List<Tuple2<String, Iterable<Sensor>>> finalList = finalRDD.collect();
//        List<Sensor> avgSensor = new ArrayList<>();
//        System.out.println("data count:"+finalList.size());
//        for (Tuple2<String, Iterable<Sensor>> tuple2 : finalList) {
//            Sensor temp = new Sensor();
//            temp.setId(tuple2._1);
//            System.out.println(temp.getId());
//            Integer count = 0;
//            Long avgDate = 0L;
//            for (Sensor s : tuple2._2) {
//
//                count+= Integer.parseInt(s.getValue());
//                avgDate = Long.parseLong(s.getDate());
//            }
//
//            System.out.println(count);
//            System.out.println("avgDate:"+avgDate);
//
//            temp.setValue((count/num)+"");
//            temp.setDate(avgDate+"");
//            avgSensor.add(temp);
//        }
//        writeMysql(avgSensor);
//
//    }
//    private static void writeMysql(List<Sensor> avgSensor){
//
//        //构建rows数据
//        List<Row> rows = new ArrayList<>();
//        for(Sensor s : avgSensor){
//            //这里要用java.sql包下的类型，而且要用timestamp类型，因为date只记录日期，time只记录时间
//            java.sql.Timestamp timestamp = new java.sql.Timestamp(Long.parseLong(s.getDate()));
////            Date date = new Date();
////            date.setTime();
//            Row row = RowFactory.create(timestamp,Double.parseDouble(s.getValue()),Integer.parseInt(s.getId()));
//            rows.add(row);
//
//        }
//
//        //构建column类型
//        List<StructField> structFields = new ArrayList<>();
//        //自增列可以忽略不写，默认值的列忽略不写即可
//        structFields.add(DataTypes.createStructField("date",DataTypes.TimestampType,true));
//        structFields.add(DataTypes.createStructField("value",DataTypes.DoubleType,true));
//        structFields.add(DataTypes.createStructField("sensor_id",DataTypes.IntegerType,true));
//
//        StructType structType = DataTypes.createStructType(structFields);
//        Dataset<Row> df = spark.createDataFrame(rows,structType);
//        df.write().mode(SaveMode.Append).jdbc(url,mysqlTableName,properties);
//
//        spark.stop();
//
//    }
//    private static void readHbase() throws IOException {
//
//        String tableName = "sensor";
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","master");
//
//
//        Scan scan = new Scan();
//        //取10条
//
//        //先测试扫描全部
//
//        conf.set(TableInputFormat.INPUT_TABLE, tableName);
//        conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
//        //获得hbase查询结果Result
//
//        JavaRDD<Tuple2<ImmutableBytesWritable,Result>> hBaseRDD = spark.sparkContext().newAPIHadoopRDD(conf,
//                TableInputFormat.class, ImmutableBytesWritable.class,
//                Result.class).toJavaRDD();
//
//
//    }
//    private static  void readMysql(){
//        String url = "jdbc:mysql://192.168.181.1:3306/jpa_test?useSSL=false&useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true&serverTimezone=GMT%2B8";
//        Properties properties = new Properties();
//        properties.put("user","root");
//        properties.put("password","root");
//        properties.put("driver","com.mysql.cj.jdbc.Driver");
//        Dataset<Row> r= spark.read().jdbc(url,"sensor_records",properties);
//        Dataset<Row> r2=r.select("date");
//        Timestamp date = r2.first().getTimestamp(0);
//        if(date != null){
//            Calendar calendar = Calendar.getInstance();
//            calendar.setTime(date);
//            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            System.out.println(format.format(date));
//        }
//
//
//        spark.stop();
//
//
//
//    }
//    private static void setProperties(){
//
//    }
//    private static void readAndWriteExample(){
//        String url = "jdbc:mysql://192.168.181.1:3306/jpa_test?useSSL=false&useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true&serverTimezone=GMT%2B8";
//        Properties properties = new Properties();
//        properties.put("user","root");
//        properties.put("password","root");
//        properties.put("driver","com.mysql.cj.jdbc.Driver");
//        System.out.println(url);
//        Dataset<Row> r= spark.read().jdbc(url,"sensors",properties);
//        Dataset<Row> r2=r.select("sensor_id");
//
//        //构建rows数据
//        Row row = RowFactory.create(1,"temp","sensor4",11.0);
//        List<Row> rows = new ArrayList<Row>();
//        rows.add(row);
//
//        //构建column类型
//        List<StructField> structFields = new ArrayList<>();
//        //自增列可以忽略不写，默认值的列忽略不写即可
////        structFields.add(DataTypes.createStructField("sensor_id",DataTypes.IntegerType,false));
//        structFields.add(DataTypes.createStructField("bridge_id",DataTypes.IntegerType,true));
//        structFields.add(DataTypes.createStructField("parameter_unit",DataTypes.StringType,true));
//        structFields.add(DataTypes.createStructField("sensor_name",DataTypes.StringType,true));
//        structFields.add(DataTypes.createStructField("threshold",DataTypes.DoubleType,true));
//
//        StructType structType = DataTypes.createStructType(structFields);
//        Dataset<Row> df = spark.createDataFrame(rows,structType);
//        //降序
////        r2.show();
////        r.orderBy(r.col("sensor_id").desc()).show();
////        r.show();
//
//        //write
//        //df.write().mode(SaveMode.Append).jdbc(url,"sensors",properties);
//
//        spark.stop();
//
//    }
//}
