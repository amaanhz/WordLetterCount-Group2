package org.wordlettercount;

// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.lang.Exception;
import java.util.regex.Pattern;

import org.apache.spark.sql.Encoders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 *  Class to run distributed Spark job to count words.
 *
 *  Weights word frequency as specified in deliverable.
 * 
 *  Since this is built into a jar that is moved to the test-data dir for execution,
 *  we can actually refer to inputs and outputs in same directory i.e. ./file.txt
 *
 *  Outputs should be both words_spark.csv and letters_spark.csv.
 */
public class SimpleApp {
    public static void main(String[] args) throws Exception {

        // NOTE: I have made a decent amount of changes, and wherever i have, i've justified them and why
        // they may speed up the program in the comments relevant to the code.


        // We actually only need two arguments: the flag and the input file path
        // So check that there are exactly two arguments, and raise an exception if not
        if (args.length != 2 || !Objects.equals(args[0], "-i")) {
                throw new Exception("Invalid arguments. USAGE: jar -i /file/to/input/path.txt");
        }
        // Check path to input file given is valid
        else if (!(new File(args[1]).isFile())) {
                throw new Exception("Given path to file is not valid.");
        }
      
        // Define input and output files
        String inputFilePath = args[1];
        String wordOutputFilePath = "./words_spark.csv";        // NOTE: i've changed this from directory to csv file so we remove overhead of messing with folders etc.
        String letterOutputFilePath = "./letters_spark.csv";    // NOTE: i've changed this from directory to csv file so we remove overhead of messing with folders etc.


        // NOTE: originally we used SparkSession, but if we are using JavaRDD's, then JavaSparkContext automatically loads an RDD
        // avoiding the overhead of converting again.
        // JavaRDD's avoid the schema processing overhead of Datasets, which we do not need as our input data is unstructured.
        SparkConf sparkConf = new SparkConf()
                .setAppName("SimpleApp")
                .setMaster("local")                             // TODO: remove, and add the below back later
                .set("spark.speculation", "true")            // Speculative execution of duplicated straggler tasks
                .set("spark.sql.adaptive.enabled", "true");  // AQE for optimising skewed data performance
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


        // TODO: check if necessary
        // Remove any existing outputs ( because spark is outputting a directory weirdly you have to delete the directory of the output path )
        // Path wordoutputPath = new Path(wordOutputFilePath);
        // FileSystem fs = wordoutputPath.getFileSystem(sparkSession.sparkContext().hadoopConfiguration());
        // if (fs.exists(wordoutputPath)) {
        //     fs.delete(wordoutputPath, true);
        // }
        // Path letteroutputPath = new Path(letterOutputFilePath);
        // if (fs.exists(letteroutputPath)) {
        //     fs.delete(letteroutputPath, true);
        // }

        // NOTE: since JavaRDD's avoid the schema processing overhead of Datasets, which we do not need as our input data is unstructured,
        // we use JavaRDD's instead of Datasets now. This helped to reduce a lot of the overhead we were seeing in results from before.
        // Read in input dataset
        JavaRDD<String> textRDD = sparkContext.textFile(inputFilePath);

        // then do the work
        // NOTE: moved logic to a new function
        optimisedWordCount(textRDD, wordOutputFilePath, sparkContext);
        // wordAndLetterCountStreamlinedImplementation(sparkSession, textDataset, wordOutputFilePath, letterOutputFilePath);

        // NOTE: we do not actually need to cache any items since we do not come back to them, and there is a slight overhead
        // of caching when working with NFS PVC's, so we avoid it now, to remove overhead and speed up the program.
        sparkContext.stop();
    }


    /**
     * An optimised version of word count, using JavaRDDs to avoid structured schema planning overhead of Datasets.
     * @param textRDD
     * @param wordCountOutputCsvPath
     */
    public static void optimisedWordCount(
        JavaRDD<String> textRDD,
        String wordCountOutputCsvPath,
        JavaSparkContext sparkContext
    ) {
        // As before, precompile regexes
        Pattern wordSplitByPunctuation = Pattern.compile("[\\s,.;:?!\"()\\[\\]{}!_-]+");
        Pattern alphabeticCharsOnly = Pattern.compile("[a-zA-Z]+");

        // NOTE: word count is made more efficient here by using only functions that are lazily evaluated in a pipeline.
        // We avoid materialising the pipeline till we have to, when we have to count the rows.
        // Get initial word counts
        JavaPairRDD<String, Integer> wordCountRDD = textRDD
                .flatMap(line -> Arrays.asList(wordSplitByPunctuation.split(line.toLowerCase())).iterator())
                .filter(word -> alphabeticCharsOnly.matcher(word).matches())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
        // NOTE: here we swap the values so that the frequency becomes the key, and then we sort with our own custom
        // comparator (as Spark has no built in mechanism yet for primary and secondary sorting, see TuplePrimarySecondaryComparator).
        // When finished, we swap back so that the word is first, and frequency is second.
        // Approach is similar to an article on StackOverflow (see TuplePrimarySecondaryComparator).
                .mapToPair(row -> new Tuple2<>(new Tuple2<>(-1 * row._2, row._1), null))
                .sortByKey(new TuplePrimarySecondaryComparator(), true, textRDD.getNumPartitions())
                .mapToPair(row -> new Tuple2<>(row._1._2, -1 * row._1._1));


        // NOTE: Since we've sorted, there's no need to apply a window or other time consuming function, and we can just use
        // zipWithIndex to add the index. We make it 1-based indexing to match the output samples.
        // In addition, we can use Tuple2 and Tuple3 items as recommended by Spark in the following documentation:
        // https://spark.apache.org/docs/latest/api/java/org/apache/spark/api/java/JavaPairRDD.html
        // Note that JavaPairRDD is an abstraction over JavaRDD<Tuple2<_, _>>.

        // Now we assign rank, by default zipWithIndex should return a Long rank item.
        JavaRDD<Tuple3<Long, String, Integer>> rankedWordsRDD = wordCountRDD
                .zipWithIndex()         // Actually produces a Tuple2 of the initial tuple, then rank i.e. ((word, freq), zero-indexed rank)
                .map(row -> new Tuple3<>(row._2+1, row._1._1, row._1._2));      // and add 1 to make it 1-indexed rank


        // Now we count and calculate thresholds as specified in deliverables. We can safely cast to long as Math.ceil and floor return integers always.
        // Remember that this is probably the bottleneck of the program as it triggers computation i.e. is not lazy.
        long totalDistinctWordCount = rankedWordsRDD.count();

        long popular = (long) Math.ceil(0.05 * totalDistinctWordCount);
        long rare = (long) Math.floor(0.95 * totalDistinctWordCount);
        long commonLowerBound = (long) Math.ceil(0.525 * totalDistinctWordCount);
        long commonUpperBound = (long) Math.floor(0.475 * totalDistinctWordCount);

        // NOTE: we can broadcast values to workers, so that we do not have to send values each time they are needed
        // i.e. we can keep them in memory. This speeds up operations.
        Broadcast<Long> popularBroadcast = sparkContext.broadcast(popular);
        Broadcast<Long> rareBroadcast = sparkContext.broadcast(rare);
        Broadcast<Long> commonLowerBoundBroadcast = sparkContext.broadcast(commonLowerBound);
        Broadcast<Long> commonUpperBoundBroadcast = sparkContext.broadcast(commonUpperBound);

        // We also can broadcast the delimiter, this may be overkill but useful for modularity of code.
        Broadcast<String> delimiterBroadcast = sparkContext.broadcast(",");

        // Then using the thresholds, we combine tuples into a string whilst adding category name.
        JavaRDD<String> outputRDD = rankedWordsRDD
                .map(row -> {
                        // Get the row and the delimiter as these are the broadcasted values that are used more than once
                        // i.e. the rest can be lazily called as they are only called once.
                        // Reminder, here row is rank, word, category, frequency
                        long rowRank = row._1();
                        String delimiter = delimiterBroadcast.value();

                        if (rowRank <= popularBroadcast.value()) {
                                return rowRank + delimiter + row._2() + delimiter + "popular" + delimiter + row._3();
                        } else if (rowRank <= commonLowerBoundBroadcast.value() && rowRank >= commonUpperBoundBroadcast.value()) {
                                return rowRank + delimiter + row._2() + delimiter + "common" + delimiter + row._3();
                        } else if (rowRank >= rareBroadcast.value()) {
                                return rowRank + delimiter + row._2() + delimiter + "rare" + delimiter + row._3();
                        } else {
                                return null;      // Equivalent to discarding row, we filter out below
                        }
                })
                .filter(row -> row != null);

        // Finally we write RDD to given path, using repartition to keep overall order of ranks
        outputRDD.repartition(1).saveAsTextFile(wordCountOutputCsvPath);

        // TODO: still writes as a directory instead of a file, do tbis instead
    }





    // TODO: create an almost identical function for letter count, and run this





    /**
     * NOTE: this function is not used anymore. We leave it in as it represents the systematic process we underwent to improve our pipeline.
     * TODO: remove at the end
     * 
     * Word and letter count at the same time. Here we attempt to streamline the process by performing all regexes
     * on one pass of each line.
     * @param sparkSession
     * @param textDataset
     * @param outputFilePath
     * @deprecated
     */
    public static void wordAndLetterCountStreamlinedImplementation(
        SparkSession sparkSession,
        Dataset<String> textDataset,
        String outputFilePathWords,
        String outputFilePathLetters
    ) {
        // This pattern splits words by spaces and all of the punctuation marks
        Pattern wordSplitByPunctuation = Pattern.compile("[\\s,.;:?!\"()\\[\\]{}!_-]+");
        // This pattern filters out words with non-alphabetic chars
        Pattern alphabeticCharsOnly = Pattern.compile("[a-zA-Z]+");
        
        // We call words and characters tokens, and this dataset is intermediate, containing both,
        // with two columns: "value" i.e. the word / char, and "type" which is "w" and "l" for word / char respectively
        Dataset<Row> tokenDataset = textDataset
                .flatMap(
                        (String line) -> {
                                // list of word/char and type "w"/"l"
                                List<TokenDatasetRow> list = new ArrayList<>();
                                String[] words = wordSplitByPunctuation.split(line);

                                // We get the words, and for each check to add as a word or not, and get chars from it
                                // whether it is a valid word or not, thus completing everything on one pass
                                for (String w: words) {
                                        if (alphabeticCharsOnly.matcher(w).matches()) {
                                                list.add(new TokenDatasetRow(w.toLowerCase(), "w"));
                                        }
                                        // then we split word up into chars, and add each char if it is a-z or A-Z
                                        for (char c: w.toCharArray()) {
                                                String s_c = String.valueOf(c);
                                                if (alphabeticCharsOnly.matcher(s_c).matches()) {
                                                        list.add(new TokenDatasetRow(s_c.toLowerCase(), "l"));
                                                }
                                        }
                                }

                                // We are required to return an iterator over the list
                                return list.iterator();
                        },
                        Encoders.bean(TokenDatasetRow.class)
                )
                .toDF("token", "type");                               // useful to cache this as we will be using for both word and letter count
        
        // So now the intermediate dataset is a list of rows e.g. "hello", "w" then "h", "l" etc.

        // Since ranking works the same for both words and letters, we can create a window to apply to both
        WindowSpec rankWindow = Window.orderBy(
                functions.col("frequency").desc(),
                functions.col("token").asc()
        );

        // And now we can efficiently split away into word count
        Dataset<Row> wordCounts = tokenDataset
                .filter("type = 'w'")           // providiong a condition expression may actually be quicker here
                .groupBy("token")
                .count()
                .withColumnRenamed("count", "frequency")
                .orderBy(
                        functions.col("frequency").desc(),
                        functions.col("token").asc()
                )
                .withColumn("rank", functions.row_number().over(rankWindow))
                .withColumnRenamed("token", "word");
        long distinctWordCount = wordCounts.count();

        // And letter count
        Dataset<Row> letterCounts = tokenDataset
                .filter("type = 'l'")           // providiong a condition expression may actually be quicker here
                .groupBy("token")
                .count()
                .withColumnRenamed("count", "frequency")
                .orderBy(
                        functions.col("frequency").desc(),
                        functions.col("token").asc()
                )
                .withColumn("rank", functions.row_number().over(rankWindow))
                .withColumnRenamed("token", "letter");
        long distinctLetterCount = letterCounts.count();

        // And categorize for word and letter counts
        Dataset<Row> wordsWithCategory = wordCounts
                .withColumn(
                        "category",
                        functions
                                .when(
                                        functions.col("rank").leq(Math.ceil(0.05 * distinctWordCount)),
                                        "popular"
                                ).when(
                                        functions.col("rank").geq(Math.floor(0.95 * distinctWordCount)),
                                        "rare"
                                ).when(
                                        functions.col("rank").between(
                                                Math.floor(0.475 * distinctWordCount),
                                                Math.ceil(0.525 * distinctWordCount)
                                        ),
                                        "common"
                                ).otherwise("")
                ).filter(functions.col("category").notEqual(""));
        
        Dataset<Row> lettersWithCategory = letterCounts
                .withColumn(
                        "category",
                        functions
                                .when(
                                        functions.col("rank").leq(Math.ceil(0.05 * distinctLetterCount)),
                                        "popular"
                                ).when(
                                        functions.col("rank").geq(Math.floor(0.95 * distinctLetterCount)),
                                        "rare"
                                ).when(
                                        functions.col("rank").between(
                                                Math.floor(0.475 * distinctLetterCount),
                                                Math.ceil(0.525 * distinctLetterCount)
                                        ),
                                        "common"
                                ).otherwise("")
                ).filter(functions.col("category").notEqual(""));

        // Finally we can write these to memory
        wordsWithCategory
                .select("rank", "word", "category", "frequency")
                .coalesce(1)
                .write()
                .option("header", "true")
                .option("delimiter", ",")       // NOTE: important that the delimiter does not have spaces
                .csv(outputFilePathWords);

        // Finally we can write these to memory
        lettersWithCategory
                .select("rank", "letter", "category", "frequency")
                .coalesce(1)
                .write()
                .option("header", "true")
                .option("delimiter", ",")       // NOTE: important that the delimiter does not have spaces
                .csv(outputFilePathLetters);
    }
}