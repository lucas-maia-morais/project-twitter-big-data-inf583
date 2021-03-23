import org.apache.spark.sql.*;
import org.apache.spark.sql.types.TimestampType;
import scala.tools.nsc.typechecker.PatternMatching;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.*;

public class Task1 {

    static public SparkSession spark;

    public static void countWords(String timestamp1, String timestamp2, String fileName) throws AnalysisException {

        // create sql context
        SQLContext sqlContext = new SQLContext(spark);

        // read file with sample tweets
        Dataset <Row> tweets = spark.read().json(fileName);

        Dataset <Row> texts = tweets.withColumn("date", from_unixtime(unix_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy")))
                .filter(col("date").gt(timestamp1))
                .filter(col("date").lt(timestamp2))
                .select(col("text"), col("date"));

        texts.show(6);

        Dataset<Row> tweetsWords = texts.withColumn("words", functions.split(texts.col("text"), " "));
        tweetsWords.show(5);

        Dataset<Row> words = tweetsWords.withColumn("words_separated", functions.explode(tweetsWords.col("words"))).select(col("words_separated"), col("date"));
        words.show(5);

        Dataset<Row> counts = words.groupBy("words_separated").count().orderBy(col("count").desc());
        counts.show(false);


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
        spark = SparkSession.builder().appName("Java Spark SQL for Twitter").config("spark.master", "local[*]").getOrCreate();
        countWords("2020-02-01 00:00:00", "2020-02-01 12:00:00",
                "data/French/2020-02-01");
    }

}
