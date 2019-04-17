package com.sparkTutorial.rdd.nasaApacheWebLogs;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHostsProblem {
	private static  LocalDate dt= LocalDate.of(1995, 7, 01); 
	private static  LocalDate dt2= LocalDate.of(1995, 8, 01); 


    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */
    	
		System.setProperty("hadoop.home.dir", "D:/");



        SparkConf conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> julyFirstLogs = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = sc.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> julyhosts= julyFirstLogs.map(line -> line.split("\t")[0]);
        JavaRDD<String> augHosts= augustFirstLogs.map(line -> line.split("\t")[0]);

        
        
        JavaRDD<String> commonHosts = julyhosts.intersection(augHosts).filter(line -> isNotHeader(line));


        JavaRDD<String> sample = commonHosts.sample(true, 0.1);

        sample.saveAsTextFile("out/nasa_logs_same_hosts.csv");
        
    }

    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }

	
}
