package com.from.data;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;


/**
 * Created by Administrator on 2018/1/4.
 */
public class Socket {
    public static void main(String[] args){

        try {
            SparkConf conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]");
            JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
            JavaReceiverInputDStream<String> lines = jssc.socketTextStream("10.111.134.36", 9999);
            JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
            JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
            JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

            // Print the first ten elements of each RDD generated in this DStream to the console
            wordCounts.print();
            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
