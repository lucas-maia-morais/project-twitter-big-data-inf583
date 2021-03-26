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

        int batchInterval = 120;
        int n = 5; // number of batches per window (increases the confidence of our estimation of Poisson coefficient)
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));
        JavaReceiverInputDStream <Status> stream = TwitterUtils.createStream(jssc);

        JavaDStream <String> txtTweets = stream.map(s ->s.getText().replace('\n', ' '));
        JavaDStream<String> wordsTweets = txtTweets.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        ///////////////////////////////////////////////////////////
        // recover word count of current and last (n-1) DStreams //
        ///////////////////////////////////////////////////////////

        JavaPairDStream <String, Integer> countCurrent = wordsTweets.mapToPair(s -> new Tuple2<>(s,1))
                .reduceByKey(Integer::sum);
        JavaPairDStream <String, Integer> countWindow = wordsTweets.mapToPair(s -> new Tuple2<>(s,1))
                .reduceByKeyAndWindow(Integer::sum, Durations.seconds(n*batchInterval), Durations.seconds(batchInterval));
        // Our idea to retrieve the word count for the last (n-1) batches before the current one:
        // since the maximum(count in window, count in current batch) = count in window,
        // and minimum(count in window, count in current batch) = count in current batch,
        // so we have count in last batches = max - min
        JavaPairDStream <String, Integer> countLastBatches = countWindow.union(countCurrent)
                .reduceByKey((a,b) -> Math.max(a,b)-Math.min(a,b));
        JavaPairDStream <String, Double> avgCountLastBatches = countLastBatches
                .mapToPair(x -> new Tuple2<>(x._1, (double) x._2/(n-1)));

        // check if the idea was correctly implemented
        // countLastBatch.filter(x -> x._2 > 0).print();

        /////////////////////////////////////
        // Apply event detection algorithm //
        /////////////////////////////////////

        // set parameter for the detection of anomalies with Poisson model
        int eta = 2;
        double alpha = 0.95;
        JavaPairDStream<String, Tuple2<Integer,Double>> joined = countCurrent.join(avgCountLastBatches);
        ///// debug and param settings - visualize threshold
        // joined.mapToPair(t ->
        //         new Tuple2<>(t._1, new Tuple2<>(t._2._1, eta * Utils.CI(t._2._2, n-1, alpha) + t._2._2)))
        //        .print();
        /////
        JavaPairDStream<String, Integer> events = joined.filter(t -> t._2._1 > eta * Utils.CI(t._2._2, n-1, alpha) + t._2._2)
                .mapToPair(t -> new Tuple2<>(t._1,t._2._1));
        events.print();


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
