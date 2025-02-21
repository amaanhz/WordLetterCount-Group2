package org.wordlettercount;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.expressions.Window;

import java.io.File;
import java.lang.Exception;

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
    public static void main(String[] args) throws Exception {
        // TODO: implement check that -i flag is present, and path to file exists and is valid type

        // We actually only need two arguments: the flag and the input file path
        // So check that there are exactly two arguments, and raise an exception if not
        if (args.length != 2 || args[0] != "-i") {
            throw new Exception("Invalid arguments. USAGE: jar -i /file/to/input/path.txt");
        }

        // Check path to input file given is valid
        // TODO: uncomment
        // if (!(new File(args[1]).isFile())) {
        //     throw new Exception("Given path to file is not valid.");
        // }

        // Define input and output files
        // TODO: change out input file path for args[1], and change outputs to match where we need to write to
        String inputFilePath = "/opt/spark/README.md";
        String wordOutputFilePath = "/example/path.csv";
        String letterOutputFilePath = "/example/path.csv";

        SparkSession sparkSession = SparkSession
            .builder()
            .appName("SimpleApp")
            .config("spark.master", "local")
            .getOrCreate();

        // then do the work
        // TODO: uncomment letterCount func call when implemented
        wordCountImplementation(sparkSession, inputFilePath, wordOutputFilePath);
        // letterCountImplementation(sparkSession, inputFilePath, letterOutputFilePath);
        
        sparkSession.stop();




        // String logFile = "/opt/spark/README.md"; // Should be some file on your system
        // SparkSession spark = SparkSession.builder().appName("SimpleApp").config("spark.master", "local").getOrCreate();
        // Dataset<String> logData = spark.read().textFile(logFile).cache();

        // long numAs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("a")).count();
        // long numBs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("b")).count();

        // System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        // spark.stop();
    }
        
    /**
     *  Implementation of word count.
     * 
     *  Should pass in session, and also stop session after (i.e. this is not handled by the function).
     *  
     *  @param sparkSession - the spark session to use
     *  @param inputFilePath
     *  @param outputFilePath
     */
    public static void wordCountImplementation(
        SparkSession sparkSession,
        String inputFilePath, 
        String outputFilePath
    ) {
        // Read in the text to the Dataset object.
        Dataset<String> textDataset = sparkSession
            .read()
            .textFile(inputFilePath)
            .cache();                   // TODO: do we need to store 500MB in memory? i.e. is cache overkill here

        Dataset<Row> wordCountsDataset = textDataset
            // TODO: check if the below logic yields the correct results or not on test data
            // For each line, we can split into words with all of the following punctuation marks
            // , . ; : ? ! " ( ) [ ] { } _
            // Note we do not split using - or ' as this is in the middle of words, and we want to
            // omit words with non-alphabetic chars such as "that's" or "non-alphabetic".
            .flatMap(
                (String line) -> Arrays.asList(line.split("[\\s,.;:?!\"()\\[\\]{}!_]+")).iterator(),
                Encoders.STRING()
            )
            // Then we filter out words with non-alphabetic chars
            .filter((FilterFunction<String>) (String word) -> word.matches("[a-zA-Z]+"))
            // Then we make everything lowercase
            .map(
                (MapFunction<String, String>) (String word) -> word.toLowerCase(),
                Encoders.STRING()
            )
            // Finally we group words together
            .groupBy("value")
            // And then count frequencies and rename columns to what we need
            .count()
            .withColumnRenamed("value", "word")
            .withColumnRenamed("count", "frequency")
            // Now we add rank column (ranking by frequency first, then alphabetic order second)
            .orderBy(functions.col("frequency").desc(), functions.col("word").asc())
            .withColumn(
                "rank",
                functions.row_number().over(
                    Window.orderBy(
                        functions.col("frequency").desc(),
                        functions.col("word").asc()
                    )
                )
            );
        
        // Count rows in dataset, required for categories
        long rowCount = wordCountsDataset.count();  // TODO: is this expensive?

        // Then categorise data
        Dataset<Row> wordCountsWithCategoryDataset = wordCountsDataset
            .withColumn(
                "category",
                functions
                    .when(
                        functions.col("rank").leq(0.05 * rowCount),
                        "popular"
                    ).when(
                        functions.col("rank").geq(0.95 * rowCount),
                        "rare"
                    ).when(
                        functions.col("rank").between(
                            0.475 * rowCount,
                            0.525 * rowCount
                        ),
                        "common"
                    ).otherwise("")
            ).filter(functions.col("category").notEqual(""));

        // and write to memory.
        wordCountsWithCategoryDataset
            .select("rank", "word", "category", "frequency")
            .repartition(1)
            .write()
            .option("header", "true")
            .option("delimiter", ",")       // NOTE: important that the delimiter does not have spaces
            .csv(outputFilePath);
    }

    /**
     * Implementation of letter count.
     */
    public static void letterCountImplementation(
        SparkSession sparkSession,
        String inputFilePath, 
        String outputFilePath
    ) {
        // TODO: implement here. you can use pretty much same approach as above
        // just make sure to only count alphabetic characters, and lowercase everything
    }
}
