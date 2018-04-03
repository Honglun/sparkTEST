package test.streaming.demo;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCount {
	
	
	public static void main(String[] args) throws InterruptedException{
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setAppName("WordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		//Arrays.as
		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[0], Integer.valueOf(args[1]));
		
		//与2.1不同，line->Arrays.asList(line.split(" ")).iterator()
		JavaDStream<String> words = lines.flatMap(line->Arrays.asList(line.split(" ")));
		JavaPairDStream<String, Integer> wordCount = words.mapToPair(word->new Tuple2<String, Integer>(word,1));
		JavaPairDStream<String, Integer> results = wordCount.reduceByKey((c1,c2)->addInt(c1,c2));
		
		results.print();
		
//		results.foreachRDD(rdd->
//		rdd.foreach(result->
//		System.out.println(result._1+" "+ result._2))
//		);
		
		jssc.start();             // Start the computation
		jssc.awaitTermination();
		
	}

	private static int addInt(int c1,int c2) {
		return c1+c2;
	}
   

}
