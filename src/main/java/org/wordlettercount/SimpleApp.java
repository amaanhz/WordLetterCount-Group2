package org.wordlettercount;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.RowFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.Exception;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
        String wordOutputFilePath = "./words_spark";
        String letterOutputFilePath = "./letters_spark";

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SimpleApp")
                .config("spark.speculation", "true")            // Speculative execution of duplicated straggler tasks
                .config("spark.sql.adaptive.enabled", "true")   // AQE for optimising skewed data performance
                .getOrCreate();

        // Remove any existing outputs ( because spark is outputting a directory weirdly you have to delete the directory of the output path )
        Path wordoutputPath = new Path(wordOutputFilePath);
        FileSystem fs = wordoutputPath.getFileSystem(sparkSession.sparkContext().hadoopConfiguration());
        if (fs.exists(wordoutputPath)) {
            fs.delete(wordoutputPath, true);
        }
        Path letteroutputPath = new Path(letterOutputFilePath);
        if (fs.exists(letteroutputPath)) {
            fs.delete(letteroutputPath, true);
        }

        // Read in input dataset
        Dataset<String> textDataset = sparkSession
                .read()
                .textFile(inputFilePath);
                // .cache();

        // then do the work
        // TODO: CHECK THAT THIS WORKS, DEBUG, AND THEN REMOVE THIS COMMENT
        wordAndLetterCountStreamlinedImplementation(sparkSession, textDataset, wordOutputFilePath, letterOutputFilePath);
        // wordCountImplementation(sparkSession, textDataset, wordOutputFilePath);
        // letterCountImplementation(sparkSession, textDataset, letterOutputFilePath);

        // remember to remove dataset from cache
        textDataset.unpersist();
        sparkSession.stop();

        // Clear any outputted junk folders
        File folder = new File(".");
        String[] names = { "letters_spark", "words_spark" };

        for (String s : names) {
            String subdir = String.format("%s/%s", folder.getPath(), s);
            File temp = new File(subdir);
            String[] filter = temp.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.substring(name.length() - 3).equals("csv");
                }
            });
            String finalpath = String.format("%s/%s", subdir, filter[0]);
            Files.move(Paths.get(finalpath), Paths.get(folder.getPath(), String.format("%s.csv", s)), StandardCopyOption.REPLACE_EXISTING);
            FileUtils.deleteDirectory(temp);
        }
    }

    /**
     *  Implementation of word count.
     *
     *  Should pass in session, and also stop session after (i.e. this is not handled by the function).
     *
     *  @param sparkSession - the spark session to use
     *  @param textDataset
     *  @param outputFilePath
     */
    public static void wordCountImplementation(
            SparkSession sparkSession,
            Dataset<String> textDataset,
            String outputFilePath
    ) {
        // Precompile the regex
        // For each line, we can split into words with all of the following punctuation marks
        // , . ; : ? ! " ( ) [ ] { } _
        Pattern wordSplitByPunctuation = Pattern.compile("[\\s,.;:?!\"()\\[\\]{}!_-]+");

        Dataset<Row> wordCountsDataset = textDataset
                // Split by precompiled regex
                .flatMap(
                        (String line) -> Arrays.asList(wordSplitByPunctuation.split(line)).iterator(),
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

        // Count rows in dataset, required for categories (an exact count isn necessary here to calculate exact percentiles)
        long rowCount = wordCountsDataset.count();

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
            .coalesce(1)
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
            Dataset<String> textDataset,
            String outputFilePath
    ) {
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
                .coalesce(1)
                .write()
                .option("header", "true")
                .option("delimiter", ",")
                .csv(outputFilePath);
    }

    /**
     * Word and letter count at the same time. Here we attempt to streamline the process by performing all regexes
     * on one pass of each line.
     * @param sparkSession
     * @param textDataset
     * @param outputFilePath
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
        StructType rowSchema = new StructType()
                .add("token", DataTypes.StringType, false)
                .add("type", DataTypes.StringType, false);
        Dataset<Row> tokenDataset = textDataset
                .flatMap(
                        (String line) -> {
                                // list of word/char and type "w"/"l"
                                List<Row> list = new ArrayList<>();
                                String[] words = wordSplitByPunctuation.split(line);

                                // We get the words, and for each check to add as a word or not, and get chars from it
                                // whether it is a valid word or not, thus completing everything on one pass
                                for (String w: words) {
                                        if (alphabeticCharsOnly.matcher(w).matches()) {
                                                list.add(RowFactory.create(w.toLowerCase(), "w"));
                                        }
                                        // then we split word up into chars, and add each char if it is a-z or A-Z
                                        for (char c: w.toCharArray()) {
                                                String s_c = String.valueOf(c);
                                                if (alphabeticCharsOnly.matcher(s_c).matches()) {
                                                        list.add(RowFactory.create(s_c.toLowerCase(), "l"));
                                                }
                                        }
                                }

                                // We are required to return an iterator over the list
                                return list.iterator();
                        },
                        RowEncoder.apply(rowSchema)
                )
                .toDF("token", "type")
                .cache();                               // useful to cache this as we will be using for both word and letter count
        
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
                .withColumnRenamed("token", "word")
                .cache();
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
                .withColumnRenamed("token", "letter")
                .cache();
        long distinctLetterCount = letterCounts.count();

        // And categorize for word and letter counts
        Dataset<Row> wordsWithCategory = wordCounts
                .withColumn(
                        "category",
                        functions
                                .when(
                                        functions.col("rank").leq(0.05 * distinctWordCount),
                                        "popular"
                                ).when(
                                        functions.col("rank").geq(0.95 * distinctWordCount),
                                        "rare"
                                ).when(
                                        functions.col("rank").between(
                                                0.475 * distinctWordCount,
                                                0.525 * distinctWordCount
                                        ),
                                        "common"
                                ).otherwise("")
                ).filter(functions.col("category").notEqual(""));
        
        Dataset<Row> lettersWithCategory = letterCounts
                .withColumn(
                        "category",
                        functions
                                .when(
                                        functions.col("rank").leq(0.05 * distinctLetterCount),
                                        "popular"
                                ).when(
                                        functions.col("rank").geq(0.95 * distinctLetterCount),
                                        "rare"
                                ).when(
                                        functions.col("rank").between(
                                                0.475 * distinctLetterCount,
                                                0.525 * distinctLetterCount
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
                .select("rank", "word", "category", "frequency")
                .coalesce(1)
                .write()
                .option("header", "true")
                .option("delimiter", ",")       // NOTE: important that the delimiter does not have spaces
                .csv(outputFilePathLetters);
    }
}