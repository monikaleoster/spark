package com.sparkTutorial.rdd.sumOfNumbers;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */
		System.setProperty("hadoop.home.dir", "D:/");

    	 SparkConf conf = new SparkConf().setAppName("primenum").setMaster("local[1]");

         JavaSparkContext sc = new JavaSparkContext(conf);

         JavaRDD<String> lines = sc.textFile("in/prime_nums.text");
         JavaRDD<String> listStrs= lines.flatMap(line->Arrays.asList(line.split("\t")).iterator());
        		// .map(str->Integer.valueOf(str));
         Integer result	= listStrs.map(lineStr->Integer.valueOf(lineStr.trim())).reduce((x,y)->x+y);
     // = listNums.reduce((x,y)->x+y);
         System.out.println("Result:"+result);
        
    }
}
