package com.vivekchutke.javaspark.apachespark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WorkingWithFlatMapsAndFilters {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("WorkingWithFlatMapsAndFilters").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<String> inputSentence = new ArrayList<String>();
        inputSentence.add("WARN: Some Warn Logs");
        inputSentence.add("ERROR: Some Error Logs");
        inputSentence.add("INFO: Some Info Logs");
        inputSentence.add("DEBUG: Some Debug Logs");
        inputSentence.add("WARN: Some Warn Logs");


        JavaRDD<String> words = sc.parallelize(inputSentence).flatMap(value1 -> Arrays.asList(value1.split(" ")).iterator());
        words.foreach(word -> System.out.println("!!! Test Values are:"+word));
        // OR prinint in a more Lamda style
        words.collect().forEach(System.out::println);

        //Use of Filter Keyword on JavaRDD. Printing only those words whose length is > 3
        JavaRDD<String> filteredWords = words.filter(word -> word.length() > 3);
        filteredWords.foreach(word -> System.out.println("@@@ Filtered Words are: "+word));
        sc.close();
    }
}
