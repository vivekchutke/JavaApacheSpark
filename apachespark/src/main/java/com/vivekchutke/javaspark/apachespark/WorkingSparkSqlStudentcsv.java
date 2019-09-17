package com.vivekchutke.javaspark.apachespark;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WorkingSparkSqlStudentcsv {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "/");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().appName("WorkingWithSparkSqlOnStudentcsv").master("local[*]")
//                .config("spark.sql.warehouse.dir", "/Users/nisum_user/Projects/SparkSpringBoot/Practise/tmp")
                .getOrCreate();

        //sparkSession.read().csv("src/main/resources/exam/students.csv");
        // To instruct the Spark there is an Header Row in the student.csv we have to use the option attribute
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exam/students.csv");
        dataset.show(100);
        Long totalNumberOfRows = dataset.count();
        System.out.println("Total Number of Rows are *****: "+totalNumberOfRows);

        //Processing a single row
        Row firstRow = dataset.first();
        // Printing the column of firstRow
        String subject = firstRow.get(2).toString();
        String subjectWithName = firstRow.getAs("subject");
        System.out.println("The Subject is @@@: "+subject);
        System.out.println("The Subject with Header Name is @@@: "+subjectWithName);


        sparkSession.close();

    }
}
