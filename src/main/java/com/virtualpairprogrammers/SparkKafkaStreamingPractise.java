package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class SparkKafkaStreamingPractise {
	
		
	public static void main(String[] args) throws InterruptedException {
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
		//JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", 8989);
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("test");

		
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(
				  sc,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );
		
		//JavaDStream<String> words = stream.map(x -> x.value());
		//words.print();
		
		JavaPairDStream<String, Integer> words = stream.mapToPair(x -> new Tuple2( x.value(),1));
	//	JavaPairDStream<String, Integer>  wordss = words.reduceByKeyAndWindow((value1,value2)-> value1+value2,Durations.minutes(10))
	//			          ;
		//JavaPairDStream<Integer,String>  wordss = words.reduceByKeyAndWindow((value1,value2)-> value1+value2,Durations.minutes(10))
		  //        .mapToPair(item->item.swap());
		
		JavaPairDStream<Integer, String>  wordss = words.reduceByKeyAndWindow((value1,value2)-> value1+value2,Durations.minutes(10))
				   .mapToPair(item->item.swap())
				   .transformToPair(rdd -> rdd.sortByKey(false));
		wordss.print();
		
		sc.start();
		sc.awaitTermination();
	}

}
