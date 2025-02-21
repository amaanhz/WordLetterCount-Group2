package org.wordlettercount;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Encoders;
import java.util.Arrays;

/**
 *  Class to run distributed Spark job to count words.
 * 
 *  Weights word frequency as specified in deliverable.
 * 
 *  TODO: modify so that the code takes in argument of input file
 *  i.e. call like ./example.jar -i ./input/file/path.txt
 * 
 *  TODO: modify so that the code writes to mounted volume, specifically
 *  to ./test-data/CloudComputingCoursework_Group2/
 *  which should contain both words_spark.csv and letters_spark.csv
 */
public class SimpleApp {
    public static void main(String[] args) {
        // Check arguments are present and valid
        // TODO: implement check that -i flag is present, and path to file exists and is valid type

        String logFile = "/opt/spark/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("SimpleApp").config("spark.master", "local").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("a")).count();
        long numBs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }

    // TODO: add equivalent function to below for letter counting

    /**
     *  Implementation of word count.
     * 
     *  Should pass in session, and also stop session after (i.e. this is not handled by the function).
     */
    public void wordCountImplementation(
        SparkSession sparkSession,
        String inputFilePath, 
        String outputFilePath
    ) {
        Dataset<String> text = sparkSession
            .read()
            .textFile(inputFilePath)
            .cache();

        // TODO: change to identify words using space and given punctuation marks
        // TODO: specifically omit words that include non-alphabetic characters
        // TODO: change to do it by lowercase
        // TODO: change to omit words that do not fit into three given categories
        // TODO: change to store words by first category (popular, common then rare) then by char ascending order i.e. a to z
        // TODO: change to have rank, word/letter, category and frequency columns
        Dataset<Row> wordCounts = text
            .flatMap(
                (String lineOfWords) -> Arrays.asList(lineOfWords.split(" ").iterator()),
                Encoders.STRING()
            )                  // TODO: flatmap here requires function that abides by the above todos
            .groupBy("value")
            .count()
            .orderBy(functions.desc("count"));

        wordCounts
            .coalesce(1)                // TODO: find more efficient way - this is expensive for large data
            .write()
            .option("header", "true")
            .option("delimiter", ",")   // NOTE: important that the delimiter does not have spaces
            .csv(outputFilePath);       // TODO: make sure we are writing to csv in the correct format i.e. look in `test-timestamp` dir in `experiments` for example, pay attention to spaces
    }
}
