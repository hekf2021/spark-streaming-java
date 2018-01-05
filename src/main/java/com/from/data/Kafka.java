package com.from.data;

import com.from.data.utils.Utils;
import com.google.common.collect.Maps;
import jodd.util.URLDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.io.UnsupportedEncodingException;
import java.util.*;


/**
 * Created by Administrator on 2018/1/4.
 */
public class Kafka {
    public static void main(String[] args) {

        try {
            SparkConf conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[3]");
            JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));
            SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
            spark.sql("use kh");
//            JavaRDD<Row> dateRdd = spark.sql("select * from info0017").toJavaRDD();
//            System.out.println("count=="+dateRdd.count());
//            System.out.println(dateRdd.collect());
            jssc.checkpoint("D:/check");
            Map<String, Integer> topicMap = new HashMap<String, Integer>();
            topicMap.put("mt02",10);
            JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, "172.16.50.21:2181", "1", topicMap);
            JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
                public String call(Tuple2<String, String> tuple2) {
                    System.out.println("bbbb==="+tuple2._2());
                    return tuple2._2();
                }
            });
            //lines.count().print();
            //处理每一批次数据，将数据写入Hive
            lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
                @Override
                public void call(JavaRDD<String> stringJavaRDD) throws Exception {

                    JavaRDD<Row> rdd = stringJavaRDD.map(new Function<String, Row>() {
                        @Override
                        public Row call(String str) throws Exception {
                            Row row = RowFactory.create(Utils.getUUID(8), str);
                            return row;
                        }
                    });
                    StructType schema = Utils.initSchema("id", "comment");
                    spark.createDataFrame(rdd, schema).write().mode(SaveMode.Append).saveAsTable("ck");
                }
            });


            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
