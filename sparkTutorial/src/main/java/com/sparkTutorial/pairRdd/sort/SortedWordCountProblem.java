package com.sparkTutorial.pairRdd.sort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SortedWordCountProblem {

	/*
	 * Create a Spark program to read the an article from in/word_count.text,
	 * output the number of occurrence of each word in descending order.
	 * 
	 * Sample output:
	 * 
	 * apple : 200 shoes : 193 bag : 176 ...
	 */

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:/");

		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster(
				"local[3]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("in/word_count.text");
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(
				line.split("\\s+")).iterator());

		Map<String, Long> wordCounts = words.countByValue();
		JavaPairRDD<String, Long> wordCountRDD = sc
				.parallelizePairs(createTupleList(wordCounts));
		JavaPairRDD<Long, String> countWordRDD = wordCountRDD
				.mapToPair(wordCount -> new Tuple2(wordCount._2, wordCount._1));
		JavaPairRDD<Long, String> sortedCountWordRDD = countWordRDD.sortByKey(false);
		JavaPairRDD<String, Long> sortedWordCountRDD = sortedCountWordRDD.mapToPair(sortedCountWord-> new Tuple2(sortedCountWord._2,sortedCountWord._1)); 


		  for (Tuple2<String, Long> wordToCount : sortedWordCountRDD.collect()) {
	            System.out.println(wordToCount._1() + " : " + wordToCount._2());
	        }
	}

	/**
	 * @param wordCounts
	 * @return
	 */
	private static List<Tuple2<String, Long>> createTupleList(
			Map<String, Long> wordCounts) {
		List<Tuple2<String, Long>> tupleList = new ArrayList<Tuple2<String, Long>>();
		for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
			tupleList.add(new Tuple2<String, Long>(entry.getKey(), entry
					.getValue()));

		}
		return tupleList;
	}
}
