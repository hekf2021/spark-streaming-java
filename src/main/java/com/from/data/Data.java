package com.from.data;

import com.google.common.collect.Maps;
import jodd.util.URLDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.io.UnsupportedEncodingException;
import java.util.*;


/**
 * Created by Administrator on 2018/1/4.
 */
public class Data {
    public static void main(String[] args) {

        try {
            SparkConf conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[3]");
            JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
//            SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
//            spark.sql("use chinacloud");
//            JavaRDD<Row> dateRdd = spark.sql("select * from info0017").toJavaRDD();
//            System.out.println("count=="+dateRdd.count());
//            System.out.println(dateRdd.collect());
            jssc.checkpoint(".");
            Map<String, Integer> topicMap = new HashMap<String, Integer>();
            topicMap.put("mt02",10);
            JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, "172.16.50.21:2181", "1", topicMap);
            JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
                public String call(Tuple2<String, String> tuple2) {
                    System.out.println("bbbb==="+tuple2._2());
                    return tuple2._2();
                }
            });
            lines.foreachRDD(x-> System.out.println(x));
            System.out.println("数据总条数：");
            lines.count().print();



            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
