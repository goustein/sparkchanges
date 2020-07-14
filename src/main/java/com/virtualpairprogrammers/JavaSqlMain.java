package com.virtualpairprogrammers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class JavaSqlMain {

	
	public static void main(String[] args) {
		
		SparkSession sparkSession =  SparkSession.builder().appName("gouse").master("local[*]").getOrCreate();
		Dataset<Row> dataSet = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");

		Dataset<String> dataSetJSON =   dataSet.toJSON();
		
	  Dataset<Row> dataSet2 = dataSet.select("year","subject");

		//dataSet.createOrReplaceTempView("gousebasha");
		
	//	Dataset<Row> dataSet2 = sparkSession. sql("select year,collect_set(subject) as subjectList from gousebasha group by year order by  year desc");
		
	//	dataSet.createOrReplaceTempView("gousebasha2");

	  
	  dataSet2 = dataSet.groupBy("year").pivot("subject").agg(functions.round(functions.avg("score"),2).alias("avgffff"),functions.round(functions.stddev("score"),2).alias("stddd"));
	  
	  
	  dataSet2.show(100);
		
		System.out.println("Total no of rows  :"+dataSet.count());
		
	}
}
