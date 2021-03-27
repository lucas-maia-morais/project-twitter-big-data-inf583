import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.unix_timestamp;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import static org.apache.spark.sql.functions.callUDF;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer;
import org.apache.spark.sql.Encoders;



public class Task9LDA {
    static public SparkSession spark;
    static public String[] vocabArray;
    
//    private Pipeline createPipeline() {
//    	Tokenizer tokenizer = new Tokenizer()
//    		.setInputCol("text")
//    		.setOutputCol("tokens");
//    	CountVectorizer countVectorizer = new CountVectorizer()
//    		.setInputCol("tokens")
//    		.setOutputCol("features")
//    		.setMinDF((Double)params.getOrDefault(params.getMinFF()))
//    		.setVocabSize((Integer)params.getOrDefault(params.getNumFeatures()));  
//    	StringIndexer tagIndexer = new StringIndexer()
//    		.setInputCol("tag")
//    		.setOutputCol("label");
//    	
//    	Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{tokenizer, countVectorizer, tagIndexer});
//    	return pipeline;
//    }
    
    public static String[] topicWords(Row row) {
    	
    	int[] s = (int[]) row.get(row.fieldIndex("termIndices"));
    	String[] topicWords = new String[s.length];
    	int i=0;
    	for (int wordIdx : s) {
    		topicWords[i] = vocabArray[wordIdx];
    		i++;
    	}
    	return topicWords;
    }
    
    public static Dataset<Row> preprocess(String timestamp1, String timestamp2, String fileName, String fileStopWords) throws AnalysisException {


        // read file with sample tweets
        Dataset <Row> tweets = spark.read().json(fileName);
        Dataset <Row> stopwords = spark.read().json(fileStopWords);
        tweets.show(false);


        Dataset <Row> texts = tweets.withColumn("date", from_unixtime(unix_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy")))
        		.select(col("id"), col("text"), col("date"))
        		.filter(col("date").gt(timestamp1))
                .filter(col("date").lt(timestamp2));

        texts.show(false);
        
        Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
        
        RegexTokenizer regexTokenizer = new RegexTokenizer()
        		.setInputCol("text")
        		.setOutputCol("words")
        		.setPattern("\\W");
        
//        spark.udf().register(
//        	      "countTokens", (Seq<?> words) -> words.size(), DataTypes.IntegerType);
        
        Dataset<Row> tokenized = tokenizer.transform(texts);
        tokenized.select("text", "words")
        		//.withColumn("tokens", callUDF("countTokens", col("words")))
        		.show(false);
        
        Dataset<Row> regexTokenized = regexTokenizer.transform(texts);
        regexTokenized.select("text", "words")
            //.withColumn("tokens", callUDF("countTokens", col("words")))
            .show(false);
        
        StopWordsRemover remover = new StopWordsRemover()
        		.setInputCol("words")
        		.setOutputCol("wordsNotFiltered")
        		.setStopWords(StopWordsRemover.loadDefaultStopWords("french"));
        
        Dataset<Row> tokenizedFiltered = remover.transform(tokenized);
        tokenizedFiltered.show(false);
        
        CountVectorizerModel cvModel = new CountVectorizer()
        		.setInputCol("wordsNotFiltered")
        		.setOutputCol("features")
        		.setVocabSize(1000)
        		//.setMinDF(2)
        		.fit(tokenizedFiltered);
        
        vocabArray = cvModel.vocabulary();
        
      
        Dataset<Row> TokenizedFilteredCounted = cvModel.transform(tokenizedFiltered);
        TokenizedFilteredCounted.show(false);
        return TokenizedFilteredCounted.select(col("id"), col("features"));
    }
    
    public static void LDACluster(Dataset<Row> df) {
    	int numTopics = 10;
    	int maxIterations = 100;
    	int vocabSize = 1000;
    	
    	
    	//long corpusSize = df.count();
    	//double mbf = 2.0/ maxIterations + 1.0 / corpusSize;
    	
    	
    	// Trains a LDA model.
    	LDA lda = new LDA()
    			.setOptimizer("online")
    			.setK(numTopics)
    			.setMaxIter(5);
    			//.setDocConcentration(-1)
    			//.setTopicConcentration(-1);
    	
    	double startTime = System.nanoTime();
    	LDAModel model = lda.fit(df);
    	double elapsed = (System.nanoTime()-startTime)/1e9;
    	
    	/**
    	 * Print results.
    	 */
    	// Print training time
    	System.out.println("Finished training LDA model.  Summary:");
    	System.out.println("Training time (sec)\t"+elapsed);
    	System.out.println("==========");

    	double ll = model.logLikelihood(df);
   		double lp = model.logPerplexity(df);
   		System.out.println("The lower bound on the log likelihood of the entire corpus: " + ll);
   		System.out.println("The upper bound on perplexity: " + lp);

//		// Describe topics.
//		Dataset<Row> topics = model.describeTopics(3);
//		System.out.println("The topics described by their top-weighted terms:");
//    	topics.show(true);
   		
   		
   		Dataset<Row> topicIndices = model.describeTopics(10);
   		//topics = topicIndices.map(col(), null);
   		//topicIndices.withColumn("topicWords", topicWords(topicIndices.("termIndices")));
   		Encoder<String> encoder = Encoders.STRING();
   		Dataset<String> wordsTopic = topicIndices.map(new MapFunction<Row,String>(){
   			@Override
   			public String call(Row value) throws Exception {
   				Integer[] s = (Integer[]) value.get(value.fieldIndex("termIndices"));
   		    	String topicWords = new String();
   		    	int i=0;
   		    	for (int wordIdx : s) {
   		    		topicWords += vocabArray[wordIdx];
   		    		if (i < s.length-1) {
   		    			topicWords += ",";
   		    		}
   		    		i++;
   		    	}
   		    	return topicWords;
   		  }
   		}, encoder);
   		wordsTopic.show(10);
   		

    	// Shows the result.
    	Dataset<Row> transformed = model.transform(df);
    	transformed.show(true);
    	
    }

	public static void main(String[] args) throws AnalysisException {
    	Logger.getLogger("org").setLevel(Level.ERROR);
	  	Logger.getLogger("akka").setLevel(Level.ERROR);

		spark = SparkSession.builder().appName("Java Spark SQL for Twitter").config("spark.master", "local[*]").getOrCreate();
		
		SQLContext sqlContext = new SQLContext(spark);
//        sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "-1");
//        sqlContext.setConf("spark.sql.broadcastTimeout",  "36000");
        sqlContext.setConf("spark.executor.memory",  "6g");
        
        Dataset<Row> datapreprocessed = preprocess("2020-02-01 00:00:00", "2020-02-01 00:05:00",
                "data/French/2020-02-01", "data/French/stop_words_french.txt");
        LDACluster(datapreprocessed);
	}

}
