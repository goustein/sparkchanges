package com.virtualpairprogrammers;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreamingPractise {
	
		
	public static void main(String[] args) throws InterruptedException {
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
		JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", 8989);
		
		
		JavaPairDStream<String, Integer> words = lines.mapToPair(x -> new Tuple2( x.split(",")[0],1));
		JavaPairDStream<String, Integer>  wordss = words.reduceByKeyAndWindow((value1,value2)-> value1+value2,Durations.minutes(10));
		wordss.print();
		sc.start();
		sc.awaitTermination();
	}

}
