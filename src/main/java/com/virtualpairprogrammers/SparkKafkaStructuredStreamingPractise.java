package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;

import scala.Tuple2;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkKafkaStructuredStreamingPractise {
	
		
	public static void main(String[] args) throws InterruptedException, StreamingQueryException {
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		
		
	
		SparkSession spark = SparkSession
				  .builder()
				  .master("local[*]")
				  .appName("JavaStructuredNetworkWordCount")
				  .getOrCreate();
		
		
		Dataset<Row> ds = spark.readStream().format("kafka")
	    .option("kafka.bootstrap.servers", "localhost:9092")
	    .option("subscribe", "test")
	    
	    .load();
		
		
		// Object dfJson = df.write.mode(SaveMode.Overwrite).json("/tmp/json/zipcodes.json")


		ds.createOrReplaceTempView("test");
		
		Dataset<Row> result =  spark.sql("select cast(value as string) course_name,sum(1) from test group by course_name ");
		
		StreamingQuery query = result.writeStream().format("console").outputMode(OutputMode.)
		.start();
       
		query.awaitTermination();
	}

}
