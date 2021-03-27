import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import scala.Tuple3;
import twitter4j.Status;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.regex.Pattern;

public class Task4 {

    /**
     * method that identifies words that may be related to a recent event. NOTE: the first n-1 results are not necessarily
     * meaningful because there is no past window for the common frequency of the words to be estimated
     * @param wordsTweets DStream with words to be processed
     * @param n number of batches considered in a larger window that we use to estimate the "common" frequency of a word
     * @param batchInterval size of batch for analysis
     * @param eta parameter that measures barely "how distant" from the estimated frequency a word should be in the
     *            current batch interval to be considered linked to an event. Negative values mean that we apply our
     *            own model
     */
    private static void eventDetectionModule(JavaDStream<String> wordsTweets, int n, int batchInterval, int eta, boolean writeToFile) {
        ///////////////////////////////////////////////////////////
        // recover word count of current and last (n-1) DStreams //
        ///////////////////////////////////////////////////////////

        JavaPairDStream <String, Integer> countCurrent = wordsTweets.mapToPair(s -> new Tuple2<>(s,1))
                .reduceByKey(Integer::sum);
        JavaPairDStream <String, Integer> countWindow = wordsTweets.mapToPair(s -> new Tuple2<>(s,1))
                .reduceByKeyAndWindow(Integer::sum, Durations.seconds((long) n*batchInterval), Durations.seconds(batchInterval));
        // Our idea to retrieve the word count for the last (n-1) batches before the current one:
        // since the maximum(count in window, count in current batch) = count in window,
        // and minimum(count in window, count in current batch) = count in current batch,
        // so we have count in last batches = max - min
        JavaPairDStream <String, Integer> countLastBatches = countWindow.union(countCurrent)
                .reduceByKey((a,b) -> Math.max(a,b)-Math.min(a,b));
        JavaPairDStream <String, Double> avgCountLastBatches = countLastBatches
                .mapToPair(x -> new Tuple2<>(x._1, (double) x._2/(n-1)));


        /////////////////////////////////////
        // Apply event detection algorithm //
        /////////////////////////////////////

        // set parameter for the detection of anomalies with Poisson model
        double alpha = 0.99;
        JavaPairDStream<String, Tuple2<Integer,Double>> joined = countCurrent.join(avgCountLastBatches);

        // detect events
        JavaPairDStream<String, Tuple3<Integer,Double,Double>> events;
        if(eta < 0) {
            // make eta proportional to |eta| and to 1 + avg count of the word in previous windows (|eta| * avg count)
            events = joined.filter(t -> t._2._1 > Math.abs(eta) * Math.sqrt(t._2._2+10) + 100 + t._2._2)
                    .mapToPair(t -> new Tuple2 <>(t._1, new Tuple3 <>(t._2._1, t._2._2, Math.sqrt(t._2._2+10) )));
        }
        else {
            events = joined.filter(t -> t._2._1 > eta * Utils.CI(t._2._2, n - 1, alpha) + t._2._2)
                    .mapToPair(t -> new Tuple2 <>(t._1, new Tuple3 <>(t._2._1, t._2._2,  Utils.CI(t._2._2, n - 1, alpha)))); // TODO
        }

        if(writeToFile) {
            DStream<String> eventWords = events.map(t -> t._1).dstream();
            eventWords.saveAsTextFiles("target/our_outputs/event_detection",
                    LocalDateTime.now().toString().replaceAll("[:.]","-"));
        }
        else
        events.print();

    }

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

        eventDetectionModule(wordsTweets, n, batchInterval, 2, false);

        jssc.start();
        try {
            jssc.awaitTermination();
        }
        catch (InterruptedException e) {
            System.err.println("InterruptedException when finalizing the Spark streaming context");
            e.printStackTrace();
        }

    }

    public static void detectEventsFromFile(String fileName, String stopWordsFileName, boolean writeToFiles) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        HashSet <String> stopWords;
        try {
            stopWords = Utils.generateStopWordsSet(stopWordsFileName);
        }
        catch (IOException e) {
            System.err.println("Could not read stop words file");
            e.printStackTrace();
            return;
        }

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Task4");
        int realDurationInSeconds = 2; // how often we will generate a batch from the file (the batch can contain data from a longer interval)
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(realDurationInSeconds));

        int intervalSize = 60;
        JavaDStream<String> txtTweets = Utils.generateStreamFromFile(jssc, fileName, intervalSize);
        assert txtTweets != null;
        // txtTweets.print();

        // identifies numbers (to be applied ** after ** split into different words)
        Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");
        // regex for unicode characters
        String unicodeCharsRegex = "\\\\u\\w\\w\\w\\w";
        // regex to map URLs adapted from https://stackoverflow.com/a/3809435 but taking into account that Twitter texts
        // already scape slashes like "\/" (so we want to map both / and (\/) as /:
        String urlRegex = "(http(s)?:(//|\\\\/\\\\/).)?(www\\.)?[-a-zA-Z0-9@:%._+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_+.~#?&/\\\\=]*)";
        // there is also a very specific and common url in the tweets data files that is not catch by the prev regex
        String commonUrlRegex = "(https:\\\\/\\\\/t\\.co\\\\/[-a-zA-Z0-9@:%._+~#=]{1,256})";
        // Finally, split into words and application of filters:
        JavaDStream<String> wordsTweets = txtTweets
                .flatMap(s -> Arrays.stream(s
                        .toLowerCase(Locale.ROOT)
                        .replaceAll(urlRegex + "|" + commonUrlRegex, "")
                        .replaceAll(unicodeCharsRegex, "")
                        .split("[\\s\\n,;.:?!'’\"{}\\[\\]()/\\\\]"))
                        .filter(w -> !w.equals("")) // remove empty strings
                        .filter(w -> !stopWords.contains(w)) // removes stop words
                        .iterator()) // split and flat
                .filter(s -> !pattern.matcher(s).matches()); // remove number-format words

        eventDetectionModule(wordsTweets, 2, realDurationInSeconds, 30, writeToFiles);

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

        // live:
        // detectLiveEvents();

        // from file:
        detectEventsFromFile("data/French/2020-02-02/2020-02-02",
                "data/French/stop_words.txt", false);
    }

}
