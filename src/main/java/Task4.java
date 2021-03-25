import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.io.IOException;
import java.util.Arrays;

public class Task4 {

    public static void detectLiveEvents() {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        String[] credentials;
        try {
            credentials = Utils.readTwitterCredentials();
        }
        catch(Exception e) {
            System.err.println("Error reading API credentials...");
            e.printStackTrace();
            return;
        }

        // Set the system properties so that Twitter4j library used by Twitter stream
        // can use them to generate OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", credentials[0]);
        System.setProperty("twitter4j.oauth.consumerSecret", credentials[1]);
        System.setProperty("twitter4j.oauth.accessToken", credentials[2]);
        System.setProperty("twitter4j.oauth.accessTokenSecret", credentials[3]);

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Task4");

        int batchInterval = 30;
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
        JavaReceiverInputDStream <Status> stream = TwitterUtils.createStream(jssc);

        JavaDStream <String> txtTweets = stream.map(s ->s.getText().replace('\n', ' '));
        JavaDStream<String> wordsTweets = txtTweets.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        //////                                                                      ///////
        // recover current and last DStreams order to apply Point-by-point Poisson Model://
        //////                                                                      ///////

        JavaPairDStream <String, Integer> countCurrent = wordsTweets.mapToPair(s -> new Tuple2<>(s,1))
                .reduceByKey(Integer::sum);
        JavaPairDStream <String, Integer> countWindow = wordsTweets.mapToPair(s -> new Tuple2<>(s,1))
                .reduceByKeyAndWindow(Integer::sum, Durations.seconds(2*batchInterval), Durations.seconds(batchInterval));
        // Our idea to retrieve the word count for the last batch before the current one:
        // since the maximum(count in window, count in current batch) = count in window,
        // and minimum(count in window, count in current batch) = count in current batch,
        // so we have count in last batch = max - min
        JavaPairDStream <String, Integer> countLastBatch = countWindow.union(countCurrent)
                .reduceByKey((a,b) -> Math.max(a,b)-Math.min(a,b));

        // check if the idea was correctly implemented
        // countLastBatch.filter(x -> x._2 > 0).print();

        jssc.start();
        try {
            jssc.awaitTermination();
        }
        catch (InterruptedException e) {
            System.err.println("InterruptedException when finalizing the Spark streaming context");
            e.printStackTrace();
        }

    }

    static public void main(String[] args) {
        detectLiveEvents();
    }

}
