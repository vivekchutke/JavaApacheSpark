package com.vivekchutke.javaspark.apachespark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class Main {

//    Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String args[]) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.OFF);
        List<Double> inputData = new ArrayList<>();
        inputData.add(34.4);
        inputData.add(23.43434);
        inputData.add(321.43);
        inputData.add(43.24);
        inputData.add(12.3232);

        // the setMaster paramter with local[*] is stating that we are using spar in local configuration and
        // that we dont have a cluster and * means use all available cores  on your machine to run this program.
        // If you do not provide [*] then that means spark will run on single thread. And running on single thread
        // will not help us get the full distributed performance of the machine
        SparkConf sparkConf = new SparkConf().setAppName("GettingStartedWithSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        sc.setLogLevel("OFF");
        //Start doing operation on spark. By loading some data onto spark
        // JavaRdd acts as an Wrapper to Scala Implementation

        JavaRDD<Double> myRddOriginal = sc.parallelize(inputData);

        //Example of Reduce
        Double result = myRddOriginal.reduce((value1, value2) -> value1 + value2);
        System.out.printf("#### ********Result is:" + result);

        // Exaple of MAP RDD
        JavaRDD<Double> sqrResult = myRddOriginal.map(value1 -> Math.sqrt(value1));
        sqrResult.foreach(value -> System.out.println("### values are:" + value));

        //Another way of printing with lamda style. When you
        //sqrResult.foreach(System.out::println); // This was throwing seralization error to overcome this check the next Line
        // The below line return a normal collection of Java so use forEach instead of foreach(In cordiation with collect() method)
        sqrResult.collect().forEach(System.out::println);

        //Straight Fwd Method
        System.out.println("####Size of the Sqrt RDD is ***:" + sqrResult.count());

        //Now an Example of Map & Reduce operation implementing the count function
        JavaRDD<Integer> mapCounter = sqrResult.map(value1 -> 1);
        Integer sqrtcounter = mapCounter.reduce((value1, value2) -> value1 + value2);
        System.out.println("####Size of the Sqrt RDD Computed Manually ***:" + sqrtcounter);

        tuplesDemo(myRddOriginal);

        //Always make sure the SparkContext is used to close in Finally block;
        sc.close();

        //Calling the other Working With RDD method
       workingWithPairRDD();
    }

    public static void tuplesDemo(JavaRDD<Double> myRddOriginal) {
        // This method demonstrates a legacy and and Scale.tuple way of storing key value pair objects
        // Legacy Way
        JavaRDD<TupleBeanObject> tupleLegacyResult = myRddOriginal.map(value1 -> new TupleBeanObject(value1));
        tupleLegacyResult.foreach(tupleBeanObject -> System.out.println("###### The Legacy Number and Sqrt of thew value are: "+ tupleBeanObject.getValue1() + " AND "+tupleBeanObject.getValueSqrRoot()));

        // Using the Java Scala way to expose out Tuple concept
        JavaRDD<Tuple2<Double,Double>> scaleTupleResult = myRddOriginal.map(value1 -> new Tuple2(value1, Math.sqrt(value1)));
        scaleTupleResult.foreach(value -> System.out.println("####### The Tuple Number and Sqrt of thew value are: "+ value._1 + " AND "+value._2));
    }

    public static void workingWithPairRDD() {
        // TestData
        List<String> inputTestData = new ArrayList<String>();
        inputTestData.add("WARN: Some Warn Logs");
        inputTestData.add("ERROR: Some Error Logs");
        inputTestData.add("INFO: Some Info Logs");
        inputTestData.add("DEBUG: Some Debug Logs");
        inputTestData.add("WARN: Some Warn Logs");
        inputTestData.add("ERROR: Some Error Logs");
        inputTestData.add("WARN: Some Warn Logs");
        inputTestData.add("DEBUG: Some Warn Logs");

        SparkConf sparkConf = new SparkConf().setAppName("WorkingWithPairRDDs").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        // parallelize or load data to spark
        JavaRDD<String> parallelizeData = javaSparkContext.parallelize(inputTestData);
        JavaPairRDD<String,Long> mapToPairRDD = parallelizeData.mapToPair(rawValue -> {
            String[] splitvalue = rawValue.split(":");
            String column1 = splitvalue[0];
            String column2 = splitvalue[1];
            return new Tuple2<>(column1, 1L);
        });

        JavaPairRDD<String, Long> sumRdd = mapToPairRDD.reduceByKey((value1, value2) -> value1+value2);
        sumRdd.foreach(value1 -> System.out.println("!!! Key:"+ value1._1 +" and Value is:"+value1._2));

        //Simplying the above 4-5 lines above as one line - > Scala way
//        parallelizeData.mapToPair(rawVale ->  new Tuple2<String, Long>(rawVale.split(":")[0],1L))
//        .reduceByKey((value1, value2) -> value1+value2)
//        .foreach(value1 -> System.out.println("!!! Key:"+ value1._1 +" and Value is:"+value1._2));


        //Using an Alternate method GroupBy which is not suggestable to use as 1. it causes crashes when dealing with big data
        // 2. It returns an iterable object which is not very friendly and we would have to use google guave packagage for the same
        parallelizeData.mapToPair(rawData -> new Tuple2<>(rawData.split(":")[0], 1L))
        .groupByKey().foreach(value1 ->
                System.out.println("!!! Using GroupBy Key:"+ value1._1 +" and Value is:"+ Iterables.size(value1._2)));

        //Close Context
        javaSparkContext.close();
    }
}
