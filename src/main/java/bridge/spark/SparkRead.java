package bridge.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class SparkRead {

    public static void main(String[] args) throws IOException, InterruptedException {
        Manager manager = new Manager();
        Thread thread = new Thread(manager);
        String cmd = "";
        Scanner scanner = new Scanner(System.in);
        thread.start();

        System.out.println("job is running");
        System.out.println("input any key to stop job");
        cmd = scanner.nextLine();
        manager.setStop(true);
        System.out.println("job is stopping,plz waiting for job to be finish");
        thread.join();
        System.out.println("job already stopped");



    }

}
