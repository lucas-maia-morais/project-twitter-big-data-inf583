import javafx.util.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Task123 {

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

        Dataset<Row> words = tweetsWords.withColumn("words_separated", explode(tweetsWords.col("words")))
                .select(col("words_separated"), col("date"))
                .filter(col("words_separated").notEqual("")); // remove empty strings

        Dataset<Row> counts = words.groupBy("words_separated").count().orderBy(col("count").desc()).filter(col("count").gt(10));

        Dataset<Row> countsWithoutStopWords = counts.join(stopwords, counts.col("words_separated").equalTo(stopwords.col(stopwords.columns()[0])), "leftanti")
                                                   .orderBy(col("count").desc())
                                                   .select(col("words_separated"), col("count"));

        Long total = texts.count();
        countsWithoutStopWords = countsWithoutStopWords.withColumn("count", round(col("count").divide(total),4));

        //counts.show(false);
        //countsWithoutStopWords.show(false);

        return new Pair<Dataset<Row>, Dataset<Row>>(counts, countsWithoutStopWords);
    }

    public static Dataset<Row> get_word_historic_in_a_day(String day) throws AnalysisException {
        Pair <Dataset<Row>, Dataset<Row>> count_words;
        Dataset<Row> words_historic;
        List<String> time_period = Arrays.asList( "night", "morning", "afternoon", "evening");
        List<String> time_hour = Arrays.asList("00:00:00", "06:00:00", "12:00:00", "18:00:00", "23:59:59");

        count_words = countWords(day + " " + time_hour.get(0), day + " "+time_hour.get(1),
                "data/French/"+day, "data/French/stop_words.txt", 100);

        words_historic = count_words.getValue()
                .withColumnRenamed("words_separated", "words")
                .withColumn("sum", col("count"))
                .withColumnRenamed("count", "count-"+ time_period.get(0));

        for (int i =1; i<4; i++) {
            count_words = countWords(day+" "+ time_hour.get(i), day+" "+time_hour.get(i+1),
                    "data/French/"+day, "data/French/stop_words.txt", 100);

            words_historic = words_historic.join(count_words.getValue(), col("words").equalTo(col("words_separated")))
                    .withColumn("new_words", when(col("words").equalTo(col("words_separated")), col("words")).otherwise(when(col("words").equalTo(null), col("words_separated")).otherwise("words")))
                    .drop("words", "words_separated")
                    .withColumn("sum", col("sum").plus(col("count")))
                    .withColumnRenamed("new_words", "words")
                    .withColumnRenamed("count", "count-"+ time_period.get(i));


        }
        words_historic = words_historic.select(col("words"), col("count-morning"), col("count-afternoon"), col("count-evening"), col("count-night"), col("sum"))
                      .withColumn("mean", col("sum").divide(time_period.size()))
                      .drop("sum")
                      .sort(col("mean").desc());

        //words_historic.show(200);

        return words_historic;

    }

    public static Dataset<Row> get_deviation (Dataset<Row> historic, String[] periods){
        String[] columns = historic.columns();
        for (int i=1; i<columns.length-1; i++){
            historic = historic.withColumn("deviation-"+periods[i-1] , round(col(columns[i]).minus(col("mean")).divide(col("mean")).multiply(100)))
                               .drop(columns[i]);

        }
        historic = historic.drop(columns[columns.length-1]);
        return historic;
    }

    public static void identify_season_words(Dataset<Row> deviation, double threshold){
        String[] columns = deviation.columns();
        Dataset<Row> words_gt = deviation.filter(col(columns[1]).gt(threshold)
                                                 .or(col(columns[2]).gt(threshold))
                                                 .or(col(columns[3]).gt(threshold))
                                                 .or(col(columns[4]).gt(threshold)));

        //words_gt = words_gt.union(deviation.filter(col(columns[i]).gt(threshold)));
        words_gt.repartition(1).rdd().saveAsTextFile("season_words_scores");
        words_gt.select("words").distinct().repartition(1).write().format("text").save("season_words_list");
        //words_gt.show(200);
    }
    public static void main(String[] args) throws AnalysisException {
        spark = SparkSession.builder().appName("Java Spark  SQL for Twitter")
                .config("spark.master", "local[*]")
                .config("spark.sql.codegen.wholeStage", "false")
                .config("spark.sql.broadcastTimeout", "3000")
                .config("spark.driver.memory", "8G")
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .getOrCreate();
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        String[] time_period = new String[]{"morning", "afternoon", "evening", "night"};
        // create sql context
        SQLContext sqlContext = new SQLContext(spark);
        sqlContext.setConf("spark.executor.memory",  "6g");
        Dataset<Row> mean_of_deviations = spark.emptyDataFrame();
        Dataset<Row> historic = spark.emptyDataFrame();
        Dataset<Row> deviation = spark.emptyDataFrame();

        for (int day = 1; day<8; day++) {
            historic = get_word_historic_in_a_day("2020-02-0"+ String.valueOf(day));
            deviation = get_deviation(historic, time_period);

            String[] columns = deviation.columns();
            if(day == 1)
                mean_of_deviations = deviation.withColumnRenamed("words", "final_words");
            else {
                mean_of_deviations = mean_of_deviations.join(deviation, mean_of_deviations.col("final_words").equalTo(deviation.col("words")), "inner")
                                                       .drop("words");
            }
            for (int col=1; col< columns.length; col++) {
                if (day == 1)
                    mean_of_deviations = mean_of_deviations.withColumn("mean-" + columns[col], lit(0));
                mean_of_deviations = mean_of_deviations.withColumn("mean-" + columns[col], col(columns[col]).plus(col("mean-" + columns[col])))
                                                       .drop(columns[col]);
            }
            System.out.println("Day "+ String.valueOf(day));
            //mean_of_deviations.show(5);
        }
        // Get columns in the right order
        //System.out.println("Finished for:");
        //mean_of_deviations.show(200);
        mean_of_deviations = mean_of_deviations.select("final_words", "mean-deviation-morning", "mean-deviation-afternoon", "mean-deviation-evening", "mean-deviation-night")
                .withColumnRenamed("final_words", "words");
        // Take the mean
        String [] mean_columns = mean_of_deviations.columns();
        for (int col=1; col<mean_columns.length; col++){
            mean_of_deviations = mean_of_deviations.withColumn(mean_columns[col], col(mean_columns[col]).divide(lit(7)));
        }
        //deviation_one.show(200);

        System.out.println("Finished select");
        //mean_of_deviations.show(200);

        identify_season_words(mean_of_deviations, 20);

    }

}
