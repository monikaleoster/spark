/***********************************************************************
 * RealEstatePractc3.java
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

import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice.AvgCount;

import scala.Tuple2;

/**
 * @author Monika.Arora
 * What is the average size of 3 bedroom flat for each location
 *
 */
public class RealEstatePractc3 {
	
	public static void main(String[] args) {
		 Logger.getLogger("org").setLevel(Level.ERROR);
			System.setProperty("hadoop.home.dir", "D:/");

	     SparkConf conf = new SparkConf().setAppName("averageSize3Bedroom").setMaster("local[1]");
	  //   SparkContext sparkContext = new SparkContext(conf);
		 JavaSparkContext sc = new JavaSparkContext(conf);


	     JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");
	     JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));
	     
	    //JavaPairRDD<String, Double> locationToPriceAverageRDD = calcAverageByAvgCount(cleanedLines);	
	     JavaPairRDD<String, Double> locationToPriceAverageRDD =calcAverageSizeOfThreeBedroom(cleanedLines);
	     for (Map.Entry<String, Double> housePriceAvgPair : locationToPriceAverageRDD.collectAsMap().entrySet()) {
	            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());
	        }

}

	/**
	 * @param cleanedLines
	 * @return
	 */
	private static JavaPairRDD<String, Double> calcAverageSizeOfThreeBedroom(
			JavaRDD<String> cleanedLines) {
		
		JavaPairRDD<String,AvgCount> locationPriceRDD= cleanedLines.filter(line-> line.split(",")[3].equals("3"))
																	.mapToPair(bb->
																				new Tuple2<String, AvgCount>(bb.split(",")[1],new AvgCount(1, Double.valueOf(bb.split(",")[6]))));
		JavaPairRDD<String,AvgCount>locationTotalPriceRDD= locationPriceRDD.
																reduceByKey((x,y)->
																			new AvgCount(x.getCount()+y.getCount(), x.getTotal()+y.getTotal()));
		return locationTotalPriceRDD.mapToPair(locationTotalPrice-> 
												new Tuple2<String,Double>(locationTotalPrice._1,locationTotalPrice._2.getTotal()/locationTotalPrice._2.getCount()));
			}
}
