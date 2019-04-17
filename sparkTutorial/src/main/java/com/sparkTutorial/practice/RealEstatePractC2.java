/***********************************************************************
 * RealEstatePractC2.java
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
import scala.Tuple2;

/**
 * @author Monika.Arora
 *
 *What is the ratio of Bathrooms to bedrooms for each location
 */
public class RealEstatePractC2 {

	public static void main(String[] args) {
		 Logger.getLogger("org").setLevel(Level.ERROR);
			System.setProperty("hadoop.home.dir", "D:/");

	     SparkConf conf = new SparkConf().setAppName("ratioBathroomBedroom").setMaster("local[1]");
	  //   SparkContext sparkContext = new SparkContext(conf);
		 JavaSparkContext sc = new JavaSparkContext(conf);


	     JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");
	     JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));
	     
	    //JavaPairRDD<String, Double> locationToPriceAverageRDD = calcAverageByAvgCount(cleanedLines);	
	     JavaPairRDD<String, Double> locationToPriceAverageRDD =calcBathroomToBedroomRatio(cleanedLines);	

		 for (Map.Entry<String, Double> housePriceAvgPair : locationToPriceAverageRDD.collectAsMap().entrySet()) {
	            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());
	        }
	     
	}

	/**
	 * @param cleanedLines
	 * @return
	 */
	private static JavaPairRDD<String, Double> calcBathroomToBedroomRatio(
			JavaRDD<String> cleanedLines) {

		JavaPairRDD<String,BathroomBedroom > locationToBathroomBedroom= cleanedLines.mapToPair(line ->
									new Tuple2<String,BathroomBedroom >(line.split(",")[1],
														new BathroomBedroom(Integer.valueOf(line.split(",")[4]),
														Integer.valueOf(line.split(",")[3]))));
		
		JavaPairRDD<String,BathroomBedroom> locationToTotalBathroomBedroom =locationToBathroomBedroom.reduceByKey((x,y)->
																		new BathroomBedroom(x.getBathroom()+y.getBathroom(), 
																		x.getBedroom()+y.getBedroom()));
		JavaPairRDD<String,Double> locationBathroomBedroomRation= locationToTotalBathroomBedroom.mapToPair(lbb-> {
		return new Tuple2<String,Double>(lbb._1, Double.valueOf(lbb._2.getBathroom())/Double.valueOf(lbb._2.getBedroom()));}
		);
		return locationBathroomBedroomRation;
	}
}
