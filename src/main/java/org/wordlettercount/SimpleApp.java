package org.wordlettercount;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.Objects;

/**
 *  Class to run distributed Spark job to count words.
 *
 *  Weights word frequency as specified in deliverable.
 *
 *  TODO: modify so that the code writes to mounted volume, specifically
 *  to ./test-data/CloudComputingCoursework_Group2/
 *  which should contain both words_spark.csv and letters_spark.csv
 */
public class SimpleApp {
    public static void main(String[] args) throws Exception {

        // We actually only need two arguments: the flag and the input file path
        // So check that there are exactly two arguments, and raise an exception if not

        if (args.length != 2 || !Objects.equals(args[0], "-i")) {
            throw new Exception("Invalid arguments. USAGE: jar -i /file/to/input/path.txt");
        }

         // Check path to input file given is valid
         if (!(new File(args[1]).isFile())) {
             throw new Exception("Given path to file is not valid.");
         }

        // Define input and output files
        String inputFilePath = args[1];
        String wordOutputFilePath = "words_spark.csv";
        String letterOutputFilePath = "letters_spark.csv"; // TODO: spark is really weird and outputs a directory with this path that contains part-00.. and _SUCCESS

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SimpleApp")
                .config("spark.master", "local")
                .getOrCreate();

        // Remove any existing outputs ( TODO: because spark is outputting a directory weirdly you have to delete the directory of the output path )
        Path wordoutputPath = new Path(wordOutputFilePath);
        FileSystem fs = wordoutputPath.getFileSystem(sparkSession.sparkContext().hadoopConfiguration());
        if (fs.exists(wordoutputPath)) {
            fs.delete(wordoutputPath, true);
        }
        Path letteroutputPath = new Path(letterOutputFilePath);
        if (fs.exists(letteroutputPath)) {
            fs.delete(letteroutputPath, true);
        }

        // then do the work
        wordCountImplementation(sparkSession, inputFilePath, wordOutputFilePath);
        letterCountImplementation(sparkSession, inputFilePath, letterOutputFilePath);

        sparkSession.stop();
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
                // For each line, we can split into words with all of the following punctuation marks
                // , . ; : ? ! " ( ) [ ] { } _
                // Note we do not split using - or ' as this is in the middle of words, and we want to
                // omit words with non-alphabetic chars such as "that's" or "non-alphabetic".
                .flatMap(
                        (String line) -> Arrays.asList(line.split("[\\s,.;:?!\"()\\[\\]{}!_-]+")).iterator(),
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
                                        functions.col("rank").leq(Math.ceil(0.05 * rowCount)),
                                        "popular"
                                ).when(
                                        functions.col("rank").geq(Math.floor(0.95 * rowCount)),
                                        "rare"
                                ).when(
                                        functions.col("rank").between(
                                                Math.floor(0.475 * rowCount),
                                                Math.ceil(0.525 * rowCount)
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
        // Read in the text to the Dataset object.
        Dataset<String> textDataset = sparkSession
                .read()
                .textFile(inputFilePath)
                .cache();

        Dataset<Row> letterCountsDataset = textDataset
                .flatMap(
                        (String line) -> Arrays.asList(line.split("")).iterator(),
                        Encoders.STRING()
                )
                // Then we filter out non alphabetic characters
                .filter((FilterFunction<String>) (String letter) -> letter.matches("[a-zA-Z]+"))
                // Then we make everything lowercase
                .map(
                        (MapFunction<String, String>) (String letter) -> letter.toLowerCase(),
                        Encoders.STRING()
                )
                // Finally we group letters together
                .groupBy("value")
                // And then count frequencies and rename columns to what we need
                .count()
                .withColumnRenamed("value", "letter")
                .withColumnRenamed("count", "frequency")
                // Now we add rank column (ranking by frequency first, then alphabetic order second)
                .orderBy(functions.col("frequency").desc(), functions.col("letter").asc())
                .withColumn(
                        "rank",
                        functions.row_number().over(
                                Window.orderBy(
                                        functions.col("frequency").desc(),
                                        functions.col("letter").asc()
                                )
                        )
                );

        // Count rows in dataset, required for categories
        long rowCount = letterCountsDataset.count();

        // Then categorise data
        // To match the sample outputs, the popular theshold and upper common threshold must be ceilinged,
        // and the rare threshold and lower common threshold must be floored
        Dataset<Row> letterCountsWithCategoryDataset = letterCountsDataset
                .withColumn(
                        "category",
                        functions
                                .when(
                                        functions.col("rank").leq(Math.ceil(0.05 * rowCount)),
                                        "popular"
                                ).when(
                                        functions.col("rank").geq(Math.floor(0.95 * rowCount)),
                                        "rare"
                                ).when(
                                        functions.col("rank").between(
                                                Math.floor(0.475 * rowCount),
                                                Math.ceil(0.525 * rowCount)
                                        ),
                                        "common"
                                ).otherwise("")
                ).filter(functions.col("category").notEqual(""));

        // and write to memory.
        letterCountsWithCategoryDataset
                .select("rank", "letter", "category", "frequency")
                .repartition(1)
                .write()
                .option("header", "true")
                .option("delimiter", ",")
                .csv(outputFilePath);
    }
}