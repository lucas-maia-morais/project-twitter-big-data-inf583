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
import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
            events = joined.filter(t -> t._2._1 > Math.abs(eta) * Math.sqrt(t._2._2+10) + 20 + t._2._2)
                    .mapToPair(t -> new Tuple2 <>(t._1, new Tuple3 <>(t._2._1, t._2._2, Math.abs(eta) * Math.sqrt(t._2._2+10) + 20 )));
        }
        else {
            events = joined.filter(t -> t._2._1 > eta * Utils.CI(t._2._2, n - 1, alpha) + t._2._2)
                    .mapToPair(t -> new Tuple2 <>(t._1, new Tuple3 <>(t._2._1, t._2._2,  eta * Utils.CI(t._2._2, n - 1, alpha))));
        }

        if(writeToFile) {
            DStream<String> eventWords = events.map(t -> t._1).dstream();
            eventWords.saveAsTextFiles("target/our_outputs/event_detection",
                    LocalDateTime.now().toString().replaceAll("[:.]","-"));
        }
        else {
            System.out.println("Ignore the first " + (n-1) + " outputs");
            events.print();
        }

    }

    private static void alternativeEventDetectionModule(JavaDStream<String> wordsTweets, int batchInterval, int eta, boolean writeToFile,
                                                        JavaStreamingContext jssc, String pathToNullModelsFolder,
                                                        HashSet<String> stopWords) {

        if(!pathToNullModelsFolder.endsWith("/")) pathToNullModelsFolder += "/";

        ArrayList <JavaDStream<String>> pastDaysStreams = new ArrayList<>();

        // Read files with tweet historic from past days:
        File folder = new File(pathToNullModelsFolder);
        String[] subFoldersNames = folder.list();
        assert subFoldersNames != null;
        for(String subFolderName : subFoldersNames) {
            File subFolder = new File(pathToNullModelsFolder + subFolderName);
            if(!subFolder.isDirectory()) continue;
            // we are in folder subFolderName/, and we want to generate a stream from an inner file with same name
            JavaDStream<String> folderTweets = Utils.generateStreamFromFile(jssc,
                    pathToNullModelsFolder + subFolderName + "/" + subFolderName,
                    batchInterval);
            pastDaysStreams.add(folderTweets);
        }
        final int nFiles = pastDaysStreams.size();
        System.out.println(nFiles + " files being used as past data");

        // preprocess these data:
//        ArrayList <JavaDStream<String>> pastDaysStreamsCleaned = new ArrayList<>();
//        pastDaysStreams.forEach(stream ->
//                pastDaysStreamsCleaned.add(Utils.preprocessing(stream, stopWords)));
        pastDaysStreams.replaceAll(stream -> Utils.preprocessing(stream, stopWords));

        // create average count of words from these data:
        ArrayList<JavaPairDStream<String, Integer>> pastDaysCounts = new ArrayList<>();
        pastDaysStreams.forEach(stream ->
                pastDaysCounts.add(stream.mapToPair(s -> new Tuple2<>(s, 1))));
        // merge and compute avg
        JavaPairDStream<String, Integer> combinedPastDaysCount = pastDaysCounts.get(0);
        for(int i = 1; i < pastDaysCounts.size(); i++) {
            combinedPastDaysCount = combinedPastDaysCount.union(pastDaysCounts.get(i));
        }
        JavaPairDStream <String, Double> avgCount = combinedPastDaysCount
                .reduceByKey(Integer::sum)
                .mapToPair(x -> new Tuple2<>(x._1, ((double) x._2)/nFiles));
//        avgCount.print();

        // count of batch being currently analyzed
        JavaPairDStream <String, Integer> countCurrent = wordsTweets.mapToPair(s -> new Tuple2<>(s,1))
                .reduceByKey(Integer::sum);

        /////////////////////////////////////
        // Apply event detection algorithm //
        /////////////////////////////////////

        // set parameter for the detection of anomalies with Poisson model
        double alpha = 0.99;
        JavaPairDStream<String, Tuple2<Integer,Double>> joined = countCurrent.join(avgCount);

        // detect events
        JavaPairDStream<String, Tuple3<Integer,Double,Double>> events;
        if(eta < 0) {
            // make eta proportional to |eta| and to 1 + avg count of the word in previous windows (|eta| * avg count)
            events = joined.filter(t -> t._2._1 > Math.abs(eta) * Math.sqrt(t._2._2+10) + 20 + t._2._2)
                    .mapToPair(t -> new Tuple2 <>(t._1, new Tuple3 <>(t._2._1, t._2._2, Math.abs(eta) * Math.sqrt(t._2._2+10) + 20 )));
        }
        else {
            events = joined.filter(t -> t._2._1 > eta * Utils.CI(t._2._2, nFiles, alpha) + t._2._2)
                    .mapToPair(t -> new Tuple2 <>(t._1, new Tuple3 <>(t._2._1, t._2._2,  eta * Utils.CI(t._2._2, nFiles, alpha))));
        }

        if(writeToFile) {
            DStream<String> eventWords = events.map(t -> t._1).dstream();
            eventWords.saveAsTextFiles("target/our_outputs/event_detection",
                    LocalDateTime.now().toString().replaceAll("[:.]","-"));
        }
        else {
            events.print();
        }


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

    public static void detectEventsFromFile(String fileName, String stopWordsFileName, boolean writeToFiles,
                                            String eventDetectionModuleName, String pathToPastDaysData) {

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

        JavaDStream<String> wordsTweets = Utils.preprocessing(txtTweets, stopWords);

        if(eventDetectionModuleName.equals("eventDetectionModule"))
            eventDetectionModule(wordsTweets, 2, realDurationInSeconds, 3, writeToFiles);
        else if(eventDetectionModuleName.equals("alternativeEventDetectionModule"))
            alternativeEventDetectionModule(wordsTweets, realDurationInSeconds, 5, writeToFiles,
                    jssc, pathToPastDaysData, stopWords);

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

        // from file (basic detection module, which uses previous intervals in a window to help detecting events):
        detectEventsFromFile("data/French/2020-02-02/2020-02-02", "data/French/stop_words.txt",
                false, "eventDetectionModule", null);

        // from file (alternative detection module, which uses previous files with past data to help detecting events):
        // We are testing with last day from our dataset, using the previous 6 as "past data"
//        detectEventsFromFile("data/French/2020-02-07/2020-02-07", "data/French/stop_words.txt",
//                false, "alternativeEventDetectionModule", "data/FrenchWithoutLastDay");
    }

}
