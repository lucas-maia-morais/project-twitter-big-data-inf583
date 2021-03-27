import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import twitter4j.Status;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

public class Utils {

    public static String[] readTwitterCredentials() throws IOException {

        String file ="src/main/resources/credentials.txt";

        String[] credentials = new String[4];

        BufferedReader reader = new BufferedReader(new FileReader(file));
        credentials[0] = reader.readLine();
        credentials[1] = reader.readLine();
        credentials[2] = reader.readLine();
        credentials[3] = reader.readLine();
        reader.close();

        return credentials;

    }

    /**
     * @param poissonMean estimated parameter of the Poisson distribution
     * @param nSamples number of samples used for the estimation (don't be afraid if this is 1 ^^')
     * @param alpha confidence level
     * @return the approx. length of the alpha-confidence interval for a Poisson distribution of sample mean poissonMean
     */
    public static double CI(double poissonMean, int nSamples, double alpha) {

        // estimate the confidence interval for nSamples * mean using nSamples * sample mean
        // (as in https://en.wikipedia.org/wiki/Poisson_distribution#Confidence_interval)
        double k = Math.max(nSamples * poissonMean, 1); // PS. ChiSquared does not accept param zero
        ChiSquaredDistribution lowerChiSquared = new ChiSquaredDistribution(2 * k);
        ChiSquaredDistribution upperChiSquared = new ChiSquaredDistribution(2 * k + 2);

        return upperChiSquared.inverseCumulativeProbability(1-alpha/2) / 2
                - lowerChiSquared.inverseCumulativeProbability(alpha/2) / 2;

    }

    public static JavaDStream<String> generateStreamFromFile(JavaStreamingContext jssc, String fileName, int interval) {

        JavaSparkContext jsc = new JavaSparkContext(jssc.ssc().sc()) ;
        FileReader fr;
        try {
            fr = new FileReader(fileName);
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        ObjectMapper mapper = new ObjectMapper(); // JSON to Java object
        // Encoder <Status> encoder = Encoders.bean(Status.class); // for spark.createDataFrame(...)
        BufferedReader br = new BufferedReader (fr);
        String line;
        ZonedDateTime currentPeriod = null;
        ArrayList<String> batch = new ArrayList<>();
        Queue<JavaRDD<String>> rdds = new LinkedList<>();
        try {
            while ((line = br.readLine()) != null) {
                HashMap <String, Object> status = mapper.readValue(line, HashMap.class);
//                Date aux = (Date) status.get("created_at");
                ZonedDateTime createdAt = ZonedDateTime.parse(status.get("created_at").toString(),
                        DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH));
//                ZonedDateTime createdAt = ZonedDateTime.ofInstant(aux.toInstant(), ZoneId.of("UTC"));
                // read date of first tweet
                if(currentPeriod == null) currentPeriod = createdAt;
                // create batch and update currentPeriod if 1 hour passed since beginning of current period
                if(currentPeriod.plusMinutes(60).isBefore(createdAt)) {
                    JavaRDD <String> rdd = jsc.parallelize(batch);
                    rdds.add(rdd);
                    batch = new ArrayList <>();
                    currentPeriod = createdAt;
                }
                batch.add(status.get("text").toString());
            }
        }
        catch(IOException e) {
            System.err.println("Error reading " + fileName);
            e.printStackTrace();
            return null;
        }
        JavaRDD<String> rdd = jsc.parallelize(batch);
        rdds.add(rdd);
        JavaDStream<String> stream = jssc.queueStream(rdds, true);

        return stream;
    }

    /**
     * Although not used for "Task 2", we decided to create a HashSet for facilitating the removal of stop words in some
     * methods
     * @param pathToFile path from content root to file containing stop words, e.g. "data/French/stop_words.txt"
     * @return set of stop words in file
     * @throws IOException if no success in reading file
     */
    public static HashSet<String> generateStopWordsSet(String pathToFile) throws IOException {
        HashSet<String> stopWords = new HashSet<>();
        BufferedReader reader = new BufferedReader(new FileReader(pathToFile));
        String line;
        while((line = reader.readLine()) != null) {
            stopWords.add(line);
        }
        return stopWords;
    }

    public static JavaDStream<String> preprocessing(JavaDStream<String> txtTweets, HashSet<String> stopWords) {
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
                        .replaceAll("https|co|rt","") // despite our efforts, there are always these 3 words
                        .split("[\\s\\n,;.:?!'’\"{}\\[\\]()/\\\\]"))
                        .filter(w -> !w.equals("")) // remove empty strings
                        .filter(w -> !stopWords.contains(w)) // removes stop words
                        .iterator()) // split and flat
                .filter(s -> !pattern.matcher(s).matches()); // remove number-format words

        return wordsTweets;
    }
}
