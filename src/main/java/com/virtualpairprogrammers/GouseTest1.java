package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class GouseTest1 {
	
	static long sum = 0 ;

	
	public static void main(String[] args) throws InterruptedException {
		
	
	SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	List<Long> dataSet = new ArrayList<>();
	for(Long i = 1L ;i <= 20L ; i++) {
		
		dataSet.add(1L);
	}
	
	JavaRDD<Long>  rdd = sc.parallelize(dataSet);
	
	rdd.foreach( value -> System.out.println(value));
	
	JavaPairRDD<Long, Integer>  mappedPairRDD = rdd.mapToPair(value -> new Tuple2(value,value*Math.random()));
	
	JavaPairRDD<Long, Integer> mappedPairRDDDistinct = mappedPairRDD.distinct();
	
	mappedPairRDDDistinct.foreach( value -> System.out.println("THread :"+Thread.currentThread().getName() +","+value ));
	
	/*
	
	JavaRDD<Long>  evenRdd =  rdd.filter(value -> value%2==0);
	
	evenRdd.foreach( value -> System.out.println("THread :"+Thread.currentThread().getName() +","+value ));

	
	
	JavaPairRDD<Long, Integer> rdd2 = rdd.mapToPair(value ->  new Tuple2<>(value,value%2 == 0 ? 1 :0));
	  
	//rdd2.foreach(value -> System.out.println(value._1+","+value._2));
	
	
	System.out.println(rdd2.count());


	/*
	long start = System.currentTimeMillis();
	long value = rdd.reduce((value1,value2) -> (long)value1+value2);
	
	
	System.out.println(value);
	System.out.println("Time taken for sparkJob ="+  (System.currentTimeMillis()-start));
	
	
	
	 start = System.currentTimeMillis();

	dataSet.forEach((value11) ->  {
		sum = sum+value11;
				});
	System.out.println(sum);

	
	System.out.println("Time taken for java job ="+  (System.currentTimeMillis()-start));
	
	
	;
	*/
	
	
	Thread.sleep(10000000000L);
	sc.close();
	}
	

}
