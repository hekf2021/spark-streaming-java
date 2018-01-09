package com.from.data;

import com.from.data.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Properties;

/**
 * Created by Administrator on 2018/1/9.
 */
public class Jdbc {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]");

        //mysql 读数到hive
        //初始化SparkSession 方便后面进行sql操作
        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://172.16.50.21:3306/abc")
                .option("dbtable", "info")
                .option("user", "root")
                .option("password", "chinacloudroot")
                .option("partitionColumn", "apk")
                .option("lowerBound", "0001")
                .option("upperBound", "0006")
                .option("numPartitions", "5")
                .load();
        System.out.println("读取分区数=="+jdbcDF.rdd().partitions().length);
        JavaRDD<Row> rdd = jdbcDF.toJavaRDD();
        StructType schema = Utils.initSchema("id", "name","apk","com");
        spark.sql("use kh");
        spark.createDataFrame(rdd, schema).write().mode(SaveMode.Append).saveAsTable("info");



    }
}
