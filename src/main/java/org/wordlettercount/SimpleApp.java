package org.wordlettercount;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "/opt/spark/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("SimpleApp").config("spark.master", "local").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("a")).count();
        long numBs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
