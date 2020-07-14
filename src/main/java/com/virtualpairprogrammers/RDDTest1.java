package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTest1 {
	
	static long sum = 0 ;

	
	public static void main(String[] args) throws InterruptedException {
		
	
	SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	List<Long> dataSet = new ArrayList<>();
	for(Long i = 1L ;i <= 10000000L ; i++) {
		
		dataSet.add(i);
	}
	

	JavaRDD<Long>  rdd = sc.parallelize(dataSet);

	
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
	
	
	Thread.sleep(10000000000L);
	sc.close();
	}
	

}
