import org.apache.spark.sql.*;
import org.apache.spark.sql.types.TimestampType;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.col;

public class Task1 {

    static public SparkSession spark;

    public static void countWords(String timestamp1, String timestamp2, String fileName) throws AnalysisException {

        // create sql context
        SQLContext sqlContext = new SQLContext(spark);

        // read file with sample tweets
        Dataset <Row> tweets = spark.read().json(fileName);

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
        countWords("Fri Jan 31 16:00:00 +0000 2020", "Fri Jan 31 23:00:00 +0000 2020",
                "data/French/2020-02-01/2020-02-01");
    }

}
