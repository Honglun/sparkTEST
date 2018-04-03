package test.streaming.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class WordCountFromKafka {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("WordCountFromKafka").setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		//jssc.checkpoint("hdfs://");
		String zk="34.213.211.112:2181,54.203.117.18:2181,54.191.251.19:2181";
		String groupId="group1";
		Map<String,Integer> topics=new HashMap<String,Integer>();
		//num partitions of kafka topic
		int numPartitionOfTopic=2;
		//num threads of spark partition
		int numThreadsOfSparkPartition=1;
		topics.put("test1", numThreadsOfSparkPartition);
		List<JavaDStream<String>> dStreamList=new ArrayList<JavaDStream<String>>();
		//every Dstream relate a kafka partition
		JavaDStream<String> lines1 = KafkaUtils.createStream(jssc, zk, groupId, topics,StorageLevel.MEMORY_ONLY_SER()).map(v->v._2);
		for(int i=1;i<numPartitionOfTopic;i++){
			JavaDStream<String> lines = KafkaUtils.createStream(jssc, zk, groupId, topics,StorageLevel.MEMORY_ONLY_SER()).map(v->v._2);
			dStreamList.add(lines);
		}
		//union all the Dstream to Dstream line1
		jssc.union(lines1, dStreamList);
		
		JavaDStream<String> words = lines1.flatMap(line->Arrays.asList(line.split(" ")));
		JavaPairDStream<String, Integer> wordCount = words.mapToPair(word->new Tuple2<String, Integer>(word,1));
		JavaPairDStream<String, Integer> results = wordCount.reduceByKey((c1,c2)->addInt(c1,c2));
		results.print();
		jssc.start();// Start the computation
		jssc.awaitTermination();
	}
	
	private static int addInt(int c1,int c2) {
		return c1+c2;
	}
   

}
