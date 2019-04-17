/***********************************************************************
 * RealEstatePractc1.java
 *
 * Copyright 2018, Kronos Incorporated. All rights reserved.
 * This software is the confidential and proprietary information of
 * Kronos, Inc. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Kronos.
 *
 * KRONOS MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF THE
 * SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE, OR NON-INFRINGEMENT. KRONOS SHALL NOT BE LIABLE FOR ANY DAMAGES
 * SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING
 * THIS SOFTWARE OR ITS DERIVATIVES.
 **********************************************************************/
package com.sparkTutorial.practice;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

import com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice.AvgCount;

import scala.Option;
import scala.Tuple2;

/**
 * To find the Average Price per Location
 * @author Monika.Arora
 *
 */
public class RealEstatePractc1 {
	public static void main(String[] args) {
		 Logger.getLogger("org").setLevel(Level.ERROR);
			System.setProperty("hadoop.home.dir", "D:/");

	     SparkConf conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[1]");
	  //   SparkContext sparkContext = new SparkContext(conf);
		 JavaSparkContext sc = new JavaSparkContext(conf);


	     JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");
	     JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));
	     
	    //JavaPairRDD<String, Double> locationToPriceAverageRDD = calcAverageByAvgCount(cleanedLines);	
	     JavaPairRDD<String, Double> locationToPriceAverageRDD =calcAverageByCountByValue(cleanedLines);	

		 for (Map.Entry<String, Double> housePriceAvgPair : locationToPriceAverageRDD.collectAsMap().entrySet()) {
	            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());
	        }
	     
	}
	
	/*public static void main(String[] args) {
		 Logger.getLogger("org").setLevel(Level.ERROR);
			System.setProperty("hadoop.home.dir", "D:/");

	     SparkConf conf = new SparkConf().setAppName("RealEstatePractice").setMaster("local[1]");
	     SparkContext sparkContext = new SparkContext(conf);
		 JavaSparkContext sc = new JavaSparkContext(conf);
		  final LongAccumulator countAccumulator = new LongAccumulator();
	        countAccumulator.register(sparkContext, Option.apply("countAccumulator"), true);

	     JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");
	     JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));
	   

			JavaPairRDD<String, Double> locationToPriceRDD= cleanedLines.mapToPair(line->new Tuple2<>(line.split(",")[1],new Double(line.split(",")[2])));
			locationToPriceRDD.persist(StorageLevel.MEMORY_ONLY());
			JavaPairRDD<String, Double> locationToTotalPriceRDD = locationToPriceRDD.reduceByKey((x,y)->{
				countAccumulator.add(1);
				return x+y;}
			);
			
			System.out.println("Accumulator>>"+ countAccumulator.count());

			Map<String, Long> locationToLocationCountMap = locationToPriceRDD.countByKey();
	     JavaPairRDD<String, Double> locationToPriceAverageRDD =locationToTotalPriceRDD.mapToPair(locationToPrice->new Tuple2<String, Double>(locationToPrice._1,locationToPrice._2/locationToLocationCountMap.get(locationToPrice._1) ));

		 for (Map.Entry<String, Double> housePriceAvgPair : locationToPriceAverageRDD.collectAsMap().entrySet()) {
	            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());
	        }
	     
	}*/
	
	
/*	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:/");

        Logger.getLogger("org").setLevel(Level.ERROR);
        
        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]");

        SparkContext sparkContext = new SparkContext(conf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        final LongAccumulator countAccumulator = new LongAccumulator();
        final Integer row= new Integer(0);

        Map<String,Integer> location2Count = new HashMap<>();
        Integer count=0;
        countAccumulator.register(sparkContext, Option.apply("Count Accumulator"), true);
        JavaRDD<String> lines = javaSparkContext.textFile("in/RealEstate.csv");
	     JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));
	     
	     JavaPairRDD<String, Double> locationToPriceRDD= cleanedLines.mapToPair(line->new Tuple2<>(line.split(",")[1],new Double(line.split(",")[2])));
			locationToPriceRDD.persist(StorageLevel.MEMORY_ONLY());
			JavaPairRDD<String, Double> locationToTotalPriceRDD = locationToPriceRDD.reduceByKey((x,y)->{
				return x+y;
				});
			locationToTotalPriceRDD.collect();
			System.out.println("Accumulator>>"+ countAccumulator.count());
	     

	}*/

	/**
	 * @param cleanedLines
	 * @return
	 */
	public static JavaPairRDD<String, Double> calcAverageByAvgCount(
			JavaRDD<String> cleanedLines) {
		long startTime= System.nanoTime();
		JavaPairRDD<String, AvgCount> locationTohousePricePairRdd = cleanedLines.mapToPair(
                 line -> new Tuple2<>(line.split(",")[1],
						new AvgCount (
								1, 
								new Double(line.split(",")[2]))));
	     JavaPairRDD<String, AvgCount> locationTohousePriceTotalPairRdd = locationTohousePricePairRdd.reduceByKey((x, y) -> 
		new AvgCount(x.getCount() + y.getCount(), x.getTotal() + y.getTotal())
		);	     
	     
		JavaPairRDD<String, Double> locationToPriceAverageRDD = locationTohousePriceTotalPairRdd
				.mapValues(x -> x.getTotal() / x.getCount());
		System.out.println("Method1>>"+(System.nanoTime()-startTime));

		return locationToPriceAverageRDD;
	}
	
	public static JavaPairRDD<String, Double> calcAverageByCountByValue(JavaRDD<String> cleanedLines){
		long startTime= System.nanoTime();

		JavaPairRDD<String, Double> locationToPriceRDD= cleanedLines.mapToPair(line->new Tuple2<>(line.split(",")[1],new Double(line.split(",")[2])));
		locationToPriceRDD.persist(StorageLevel.MEMORY_ONLY());
		JavaPairRDD<String, Double> locationToTotalPriceRDD = locationToPriceRDD.reduceByKey((x,y)->{
			return x+y;}
		);
		
		//Map<String, Double> locationToTotalPriceMap = locationToPriceRDD.collectAsMap();

		Map<String, Long> locationToLocationCountMap = locationToPriceRDD.countByKey();
		//System.out.println("Method2>>"+(System.nanoTime()-startTime));

		return	locationToTotalPriceRDD.mapToPair(locationToPrice->new Tuple2<String, Double>(locationToPrice._1,locationToPrice._2/locationToLocationCountMap.get(locationToPrice._1) ));
		
	}

	/**
	 * @return
	 */
	private static PairFunction getPairFunction() {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
