import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.TimestampType;
import scala.tools.nsc.typechecker.PatternMatching;

import javax.xml.crypto.Data;
import java.sql.Timestamp;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Task1_2 {

    static public SparkSession spark;

    public static void countWords(String timestamp1, String timestamp2, String fileName, String fileStopWords) throws AnalysisException {


        // read file with sample tweets
        Dataset <Row> tweets = spark.read().json(fileName);
        Dataset <Row> stopwords = spark.read().json(fileStopWords);
        tweets.show(3);


        Dataset <Row> texts = tweets.withColumn("date", from_unixtime(unix_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy")))
                .filter(col("date").gt(timestamp1))
                .filter(col("date").lt(timestamp2))
                .select(col("text"), col("date"));

        texts.show(6);

        Dataset<Row> tweetsWords = texts.withColumn("words", split(lower(texts.col("text")), "[\\s,;.:?!'’\"/\\\\]"));
        tweetsWords.show(5);


        Dataset<Row> words = tweetsWords.withColumn("words_separated", explode(tweetsWords.col("words")))
                .select(col("words_separated"), col("date"))
                .filter(col("words_separated").notEqual("")); // remove empty strings
        words.show(5);

        Dataset<Row> counts = words.groupBy("words_separated").count().orderBy(col("count").desc());
        counts.show(false);


        Dataset<Row> countsWithoutStopWords = counts.join(stopwords, counts.col("words_separated").equalTo(stopwords.col(stopwords.columns()[0])), "leftanti")
                                                   .orderBy(col("count").desc())
                                                   .select(col("words_separated"), col("count"));
        countsWithoutStopWords.show(false);

        /*
        Dataset<Row> countsWithoutStopWords = counts.filter((FilterFunction<Row>)  row -> {
            // keep the row if the variable name column does not contain a value that should be filtered
            String variable = row.getAs("words_separated");

            return stop_words.contains(variable);
            });
            countsWithoutStopWords.show(10);
        */
        // NAO FUNFA 100%
//        Dataset <Row> filtered = tweets.filter(col("created_at").cast("timestamp").$greater(timestamp1));
//        filtered.show(6);

        // NAO FUNFA 100%
        // filter tweets on time interval
//        tweets.createGlobalTempView("tweets");
//        spark.sql("SELECT * FROM global_temp.tweets " +
//                "WHERE  TO_TIMESTAMP(created_at,'%W %M %e %H:%i:%s +0000 %Y') >= " +
//                "TO_TIMESTAMP('" + timestamp1 + "','%W %M %e %H:%i:%s +0000 %Y') " +
//                "AND TO_TIMESTAMP(created_at,'%W %M %e %H:%i:%s +0000 %Y') <=" +
//                " TO_TIMESTAMP('" + timestamp2 + "','%W %M %e %H:%i:%s +0000 %Y') ").show(10);
    }

    public static void main(String[] args) throws AnalysisException {
        spark = SparkSession.builder().appName("Java Spark  SQL for Twitter")
                .config("spark.master", "local[*]")
                // .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .getOrCreate();

        // create sql context
        SQLContext sqlContext = new SQLContext(spark);
        // sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "-1");
        // sqlContext.setConf("spark.sql.broadcastTimeout",  "36000");
        sqlContext.setConf("spark.executor.memory",  "6g");


        countWords("2020-02-01 00:00:00", "2020-02-01 06:00:00",
                "data/French/2020-02-01", "data/French/stop_words.txt");
    }

}
