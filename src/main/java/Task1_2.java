import javafx.util.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Task1_2 {

    static public SparkSession spark;

    public static Pair<Dataset<Row>, Dataset<Row>> countWords(String timestamp1, String timestamp2, String fileName, String fileStopWords, int top_x) throws AnalysisException {


        // read file with sample tweets
        Dataset <Row> tweets = spark.read().json(fileName);
        Dataset <Row> stopwords = spark.read().json(fileStopWords);


        Dataset <Row> texts = tweets.withColumn("date", from_unixtime(unix_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy")))
                .filter(col("date").gt(timestamp1))
                .filter(col("date").lt(timestamp2))
                .select(col("text"), col("date"));

        Dataset<Row> tweetsWords = texts.withColumn("words", split(lower(texts.col("text")), "[\\s\\n,;.:?!'’\"/\\\\]"));
        tweetsWords.show(5);


        Dataset<Row> words = tweetsWords.withColumn("words_separated", explode(tweetsWords.col("words")))
                .select(col("words_separated"), col("date"))
                .filter(col("words_separated").notEqual("")); // remove empty strings
        words.show(5);

        Dataset<Row> counts = words.groupBy("words_separated").count().orderBy(col("count").desc());

        Dataset<Row> countsWithoutStopWords = counts.join(stopwords, counts.col("words_separated").equalTo(stopwords.col(stopwords.columns()[0])), "leftanti")
                                                   .orderBy(col("count").desc())
                                                   .select(col("words_separated"), col("count"));

        //countsWithoutStopWords.withColumn("count", col("count").divide(lit(sum(col("count")))).multiply(lit(1000)));

        counts.show(false);
        countsWithoutStopWords.show(false);

        return new Pair<Dataset<Row>, Dataset<Row>>(counts, countsWithoutStopWords);
    }

    public static void main(String[] args) throws AnalysisException {
        spark = SparkSession.builder().appName("Java Spark  SQL for Twitter")
                .config("spark.master", "local[*]")
                .getOrCreate();
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        // create sql context
        SQLContext sqlContext = new SQLContext(spark);
        sqlContext.setConf("spark.executor.memory",  "6g");

        Pair <Dataset<Row>, Dataset<Row>> count_words;
        Dataset<Row> words_historic;
        List<String> time_period = Arrays.asList( "night", "morning", "afternoon", "evening");
        List<String> time_hour = Arrays.asList("00:00:00", "06:00:00", "12:00:00", "18:00:00", "24:00:00");

        count_words = countWords("2020-02-01 "+ time_hour.get(0), "2020-02-01 "+time_hour.get(1),
                "data/French/2020-02-01", "data/French/stop_words.txt", 100);

        words_historic = count_words.getValue()
                .withColumnRenamed("words_separated", "words")
                .withColumn("sum", col("count"))
                .withColumnRenamed("count", "count-"+ time_period.get(0));

        for (int i =1; i<4; i++) {
            count_words = countWords("2020-02-01 "+ time_hour.get(i), "2020-02-01 "+time_hour.get(i+1),
                    "data/French/2020-02-01", "data/French/stop_words.txt", 100);

            words_historic = words_historic.join(count_words.getValue(), col("words").equalTo(col("words_separated")))
                                           .withColumn("new_words", when(col("words").equalTo(col("words_separated")), col("words")).otherwise(when(col("words").equalTo(null), col("words_separated")).otherwise("words")))
                                           .drop("words", "words_separated")
                                           .withColumn("sum", col("sum").plus(col("count")))
                                           .withColumnRenamed("new_words", "words")
                                           .withColumnRenamed("count", "count-"+ time_period.get(i));


        }
        words_historic.select(col("words"), col("count-morning"), col("count-afternoon"), col("count-evening"), col("count-night"), col("sum"))
                .sort(col("sum").desc()).show(200);

    }

}
