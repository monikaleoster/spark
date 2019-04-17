package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class AverageHousePriceProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           output the average price for houses with different number of bedrooms.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

           (3, 325000)
           (1, 266356)
           (2, 325000)
           ...

           3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
         */
    	
    	
    	
		System.setProperty("hadoop.home.dir", "D:/");


        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("averageHouse").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> cleanedLines = sc.textFile("in/RealEstate.csv").filter(line->isNotHeader(line));

        JavaPairRDD<Integer, AverageCount> BedroomToPriceMap = cleanedLines.mapToPair(getCountToPricePair());

        JavaPairRDD<Integer, AverageCount> bedRoomAvgCount= BedroomToPriceMap.reduceByKey((x,y)->new AverageCount(x.getCount()+y.getCount(),x.getPrice()+y.getPrice()));
      //  JavaPairRDD<Integer, Double> BedroomToAvgPriceMapRdd = BedroomToPriceMap.reduceByKey((Function2<Integer, AverageCount, Double>) (x, y) -> ());

        JavaPairRDD<Integer, Double> bedRoomAvg=bedRoomAvgCount.mapValues(avgCount->avgCount.getPrice()/avgCount.getCount()).sortByKey();
       for (Map.Entry<Integer, Double> bedroomAvgPrice : bedRoomAvg.collectAsMap().entrySet()) {
            System.out.println(bedroomAvgPrice.getKey() + " : " + bedroomAvgPrice.getValue());

        }
    }

	/**
	 * @return
	 */
	private static boolean isNotHeader(String line) {
        return !(line.startsWith("MLS"));

	}

	/**
	 * @return
	 */
	private static PairFunction<String, Integer, AverageCount> getCountToPricePair() {
		

		return (PairFunction<String, Integer, AverageCount>)s -> new Tuple2<>(Integer.valueOf(s.split(",")[3]), new AverageCount(1, Double.valueOf(s.split(",")[2])));

	}
    }


